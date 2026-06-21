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

// akkengine/src/engine/sstable/SSTWriter.cpp
#include "akk/engine/sstable/SSTWriter.hpp"

#include <algorithm>
#include <bit>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <string>

#include <zstd.h>

#include "akk/core/record/SSTHdr32.hpp"
#include "akk/cpu/CRC32C.hpp"

namespace akkaradb::engine::sst {
    namespace {
        [[nodiscard]] uint32_t crc32c(std::span<const uint8_t> bytes) noexcept {
            return cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size());
        }

        template <typename T>
        void appendPod(std::vector<uint8_t>& out, const T& value) {
            const auto* p = reinterpret_cast<const uint8_t*>(&value);
            out.insert(out.end(), p, p + sizeof(T));
        }

        [[nodiscard]] size_t sharedPrefixLen(std::span<const uint8_t> a, std::span<const uint8_t> b) noexcept {
            const size_t n = std::min(a.size(), b.size());
            size_t i = 0;
            while (i < n && a[i] == b[i]) { ++i; }
            return i;
        }

        void writeExact(std::ofstream& out, const void* data, size_t size) {
            out.write(reinterpret_cast<const char*>(data), static_cast<std::streamsize>(size));
            if (!out) { throw std::runtime_error("SSTWriter: write failed"); }
        }

        [[nodiscard]] uint32_t nextPow2U32(uint64_t value) noexcept {
            if (value <= 2) { return 2; }
            if (value > (1ULL << 31)) { return 1u << 31; }
            return static_cast<uint32_t>(std::bit_ceil(value));
        }

        struct BloomBuild {
            SSTBloomHeaderV2 header{};
            std::vector<uint8_t> bits;

            explicit BloomBuild(size_t entries, uint32_t bitsPerKey) {
                const uint64_t requestedBits = std::max<uint64_t>(512, static_cast<uint64_t>(entries) * std::max<uint32_t>(1, bitsPerKey));
                header.numBits = nextPow2U32(requestedBits);
                header.numHashes = std::max<uint32_t>(1, static_cast<uint32_t>(static_cast<double>(bitsPerKey) * 0.69));
                header.bitsSize = header.numBits / 8;
                bits.assign(header.bitsSize, 0);
            }

            void add(uint64_t fp64) {
                const uint32_t mask = header.numBits - 1;
                const uint32_t h1 = static_cast<uint32_t>(fp64);
                const uint32_t h2 = (static_cast<uint32_t>(fp64 >> 32) | 1u);
                for (uint32_t i = 0; i < header.numHashes; ++i) {
                    const uint32_t bit = (h1 + i * h2) & mask;
                    bits[bit >> 3] = static_cast<uint8_t>(bits[bit >> 3] | (1u << (bit & 7u)));
                }
            }
        };

        struct PendingBlock {
            std::vector<uint8_t> raw;
            std::vector<uint32_t> offsets;
            std::vector<uint8_t> firstKey;
            std::vector<uint8_t> lastKey;
            uint64_t firstSeq = 0;
            uint64_t lastSeq = 0;
            uint64_t firstFp = 0;
            uint64_t lastFp = 0;
            uint64_t firstMini = 0;
            uint64_t lastMini = 0;

            [[nodiscard]] bool empty() const noexcept { return offsets.empty(); }
        };

        [[nodiscard]] uint32_t addKey(std::vector<uint8_t>& arena, std::span<const uint8_t> key) {
            if (arena.size() > UINT32_MAX) { throw std::runtime_error("SSTWriter: key arena too large"); }
            const uint32_t off = static_cast<uint32_t>(arena.size());
            arena.insert(arena.end(), key.begin(), key.end());
            return off;
        }

        void appendRecord(PendingBlock& block, const core::RecordView& rec) {
            if (block.raw.size() > UINT32_MAX) { throw std::runtime_error("SSTWriter: block too large"); }
            block.offsets.push_back(static_cast<uint32_t>(block.raw.size()));

            const auto key = rec.key();
            const auto value = rec.value();
            core::SSTHdr32 hdr{
                .seq = rec.seq(),
                .kLen = static_cast<uint16_t>(key.size()),
                .vLen = static_cast<uint16_t>(value.size()),
                .flags = rec.flags(),
                .reserved0 = 0,
                .reserved1 = 0,
                .keyFp64 = rec.keyFp64(),
                .miniKey = rec.miniKey()
            };

            if (key.size() > UINT16_MAX || value.size() > UINT16_MAX) {
                throw std::invalid_argument("SSTWriter: key/value length exceeds u16");
            }

            appendPod(block.raw, hdr);
            block.raw.insert(block.raw.end(), key.begin(), key.end());
            block.raw.insert(block.raw.end(), value.begin(), value.end());
            const uint64_t padded = alignUpU64(static_cast<uint64_t>(block.raw.size()), 8);
            block.raw.resize(static_cast<size_t>(padded), 0);

            if (block.offsets.size() == 1) {
                block.firstKey.assign(key.begin(), key.end());
                block.firstSeq = hdr.seq;
                block.firstFp = hdr.keyFp64;
                block.firstMini = hdr.miniKey;
            }
            block.lastKey.assign(key.begin(), key.end());
            block.lastSeq = hdr.seq;
            block.lastFp = hdr.keyFp64;
            block.lastMini = hdr.miniKey;
        }

        [[nodiscard]] std::vector<uint8_t> compressOrRaw(const std::vector<uint8_t>& raw, SSTWriter::Codec codec, uint32_t& flags) {
            const uint32_t extraFlags = flags & ~SST_BLOCK_FLAG_RAW;
            flags = extraFlags | SST_BLOCK_FLAG_RAW;
            if (codec != SSTWriter::Codec::ZSTD || raw.empty()) { return raw; }

            const size_t bound = ZSTD_compressBound(raw.size());
            std::vector<uint8_t> compressed(bound);
            const size_t n = ZSTD_compress(compressed.data(), compressed.size(), raw.data(), raw.size(), 1);
            if (ZSTD_isError(n) || n >= raw.size()) { return raw; }
            compressed.resize(n);
            flags = extraFlags | SST_BLOCK_FLAG_COMPRESSED;
            return compressed;
        }

        struct BlockPayload {
            std::vector<uint8_t> data;
            std::vector<uint32_t> offsets;
            uint32_t flags = SST_BLOCK_FLAG_RAW;
        };

        [[nodiscard]] BlockPayload encodePrefixCompressedBlock(const PendingBlock& block) {
            BlockPayload out;
            out.flags = SST_BLOCK_FLAG_PREFIX_COMPRESSED;
            out.data.reserve(block.raw.size());
            out.offsets.reserve(block.offsets.size());

            std::vector<uint8_t> prevKey;
            for (size_t i = 0; i < block.offsets.size(); ++i) {
                const uint32_t off = block.offsets[i];
                if (off + sizeof(core::SSTHdr32) > block.raw.size()) { throw std::runtime_error("SSTWriter: corrupt source block header"); }
                const auto* hdr = reinterpret_cast<const core::SSTHdr32*>(block.raw.data() + off);
                const uint8_t* keyPtr = block.raw.data() + off + sizeof(core::SSTHdr32);
                const uint8_t* valPtr = keyPtr + hdr->kLen;
                if (valPtr + hdr->vLen > block.raw.data() + block.raw.size()) { throw std::runtime_error("SSTWriter: corrupt source block payload"); }

                std::span<const uint8_t> key{keyPtr, hdr->kLen};
                const uint16_t shared = static_cast<uint16_t>(std::min<size_t>(sharedPrefixLen(prevKey, key), UINT16_MAX));
                const size_t suffixLen = key.size() - shared;

                out.offsets.push_back(static_cast<uint32_t>(out.data.size()));

                core::SSTHdr32 encHdr = *hdr;
                encHdr.reserved0 = 0;
                encHdr.reserved1 = shared;
                appendPod(out.data, encHdr);
                out.data.insert(out.data.end(), key.begin() + shared, key.end());
                out.data.insert(out.data.end(), valPtr, valPtr + hdr->vLen);
                const uint64_t padded = alignUpU64(static_cast<uint64_t>(out.data.size()), 8);
                out.data.resize(static_cast<size_t>(padded), 0);

                (void)suffixLen;
                prevKey.assign(key.begin(), key.end());
            }

            return out;
        }
    } // namespace

    SSTWriter::Result SSTWriter::write(const std::filesystem::path& path, std::span<const core::RecordView> records) {
        return write(path, records, Options{});
    }

    SSTWriter::Result SSTWriter::write(
        const std::filesystem::path& path,
        std::span<const core::RecordView> records,
        const Options& options
    ) {
        if (records.empty()) { throw std::invalid_argument("SSTWriter::write: records must be non-empty"); }
        if (options.blockSize < 4096 || (options.blockSize & 7u) != 0) {
            throw std::invalid_argument("SSTWriter::write: blockSize must be >=4096 and 8-byte aligned");
        }

        for (size_t i = 1; i < records.size(); ++i) {
            if (records[i - 1].compareKey(records[i]) > 0) {
                throw std::invalid_argument("SSTWriter::write: records must be sorted by key");
            }
        }

        if (path.has_parent_path()) { std::filesystem::create_directories(path.parent_path()); }
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        if (!out) { throw std::runtime_error("SSTWriter: cannot open " + path.string()); }

        SSTFileHeaderV2 header{};
        writeExact(out, &header, sizeof(header));

        Result result;
        result.path = path;
        result.entryCount = records.size();
        result.firstKey.assign(records.front().key().begin(), records.front().key().end());
        result.lastKey.assign(records.back().key().begin(), records.back().key().end());

        BloomBuild bloom(records.size(), options.bloomBitsPerKey);
        for (const auto& rec : records) {
            bloom.add(rec.keyFp64());
            result.minSeq = std::min(result.minSeq, rec.seq());
            result.maxSeq = std::max(result.maxSeq, rec.seq());
        }

        std::vector<SSTBlockIndexEntryV2> index;
        std::vector<uint8_t> keyArena;
        PendingBlock block;

        auto flushBlock = [&]() {
            if (block.empty()) { return; }

            const BlockPayload encoded = encodePrefixCompressedBlock(block);
            const bool usePrefixCompressed = !encoded.data.empty() && encoded.data.size() < block.raw.size();
            const std::vector<uint8_t>& rawPayload = usePrefixCompressed ? encoded.data : block.raw;
            const std::vector<uint32_t>& rawOffsets = usePrefixCompressed ? encoded.offsets : block.offsets;

            uint32_t blockFlags = usePrefixCompressed ? SST_BLOCK_FLAG_PREFIX_COMPRESSED : 0;
            std::vector<uint8_t> payload = compressOrRaw(rawPayload, options.codec, blockFlags);
            std::vector<uint8_t> offsetsBytes;
            offsetsBytes.reserve(rawOffsets.size() * sizeof(uint32_t));
            for (const uint32_t off : rawOffsets) { appendPod(offsetsBytes, off); }

            const uint64_t blockOffset = static_cast<uint64_t>(out.tellp());
            SSTBlockHeaderV2 bh{};
            bh.headerSize = sizeof(SSTBlockHeaderV2);
            bh.flags = blockFlags;
            bh.recordCount = static_cast<uint32_t>(block.offsets.size());
            bh.compressedSize = static_cast<uint32_t>(payload.size());
            bh.uncompressedSize = static_cast<uint32_t>(rawPayload.size());
            bh.offsetsSize = static_cast<uint32_t>(offsetsBytes.size());
            bh.firstSeq = block.firstSeq;
            bh.lastSeq = block.lastSeq;
            bh.firstKeyFp64 = block.firstFp;
            bh.lastKeyFp64 = block.lastFp;

            std::vector<uint8_t> crcInput;
            crcInput.reserve(payload.size() + offsetsBytes.size());
            crcInput.insert(crcInput.end(), payload.begin(), payload.end());
            crcInput.insert(crcInput.end(), offsetsBytes.begin(), offsetsBytes.end());
            bh.crc32c = crc32c(crcInput);

            writeExact(out, &bh, sizeof(bh));
            writeExact(out, payload.data(), payload.size());
            writeExact(out, offsetsBytes.data(), offsetsBytes.size());

            const uint64_t next = alignUpU64(static_cast<uint64_t>(out.tellp()), 8);
            const size_t pad = static_cast<size_t>(next - static_cast<uint64_t>(out.tellp()));
            if (pad > 0) {
                const uint8_t zeros[8]{};
                writeExact(out, zeros, pad);
            }

            const uint32_t firstKeyOff = addKey(keyArena, block.firstKey);
            const uint32_t lastKeyOff = addKey(keyArena, block.lastKey);
            index.push_back(
                SSTBlockIndexEntryV2{
                    .blockOffset = blockOffset,
                    .blockSize = static_cast<uint32_t>(next - blockOffset),
                    .uncompressedSize = static_cast<uint32_t>(block.raw.size()),
                    .firstMiniKey = block.firstMini,
                    .lastMiniKey = block.lastMini,
                    .firstKeyFp64 = block.firstFp,
                    .lastKeyFp64 = block.lastFp,
                    .firstKeyOffset = firstKeyOff,
                    .firstKeyLen = static_cast<uint32_t>(block.firstKey.size()),
                    .lastKeyOffset = lastKeyOff,
                    .lastKeyLen = static_cast<uint32_t>(block.lastKey.size()),
                    .recordCount = static_cast<uint32_t>(block.offsets.size()),
                    .flags = blockFlags
                }
            );

            block = PendingBlock{};
        };

        for (const auto& rec : records) {
            const uint64_t estimated = alignUpU64(32 + rec.keySize() + rec.valueSize(), 8);
            if (!block.empty() && block.raw.size() + estimated > options.blockSize) { flushBlock(); }
            appendRecord(block, rec);
        }
        flushBlock();

        const uint64_t indexOffset = static_cast<uint64_t>(out.tellp());
        for (const auto& entry : index) { writeExact(out, &entry, sizeof(entry)); }

        const uint64_t keyArenaOffset = static_cast<uint64_t>(out.tellp());
        if (!keyArena.empty()) { writeExact(out, keyArena.data(), keyArena.size()); }

        const uint64_t bloomOffset = static_cast<uint64_t>(out.tellp());
        writeExact(out, &bloom.header, sizeof(bloom.header));
        writeExact(out, bloom.bits.data(), bloom.bits.size());

        const uint64_t footerOffset = static_cast<uint64_t>(out.tellp());
        SSTFooterV2 footer{};
        footer.file_size = footerOffset + sizeof(SSTFooterV2);
        footer.indexOffset = indexOffset;
        footer.keyArenaOffset = keyArenaOffset;
        footer.bloomOffset = bloomOffset;
        footer.magic = SST_FOOTER_MAGIC_V2;
        footer.version = SST_VERSION_V2;

        header.magic = SST_MAGIC_V2;
        header.version = SST_VERSION_V2;
        header.headerSize = sizeof(SSTFileHeaderV2);
        header.flags = options.codec == Codec::ZSTD ? SST_FILE_FLAG_BLOCK_ZSTD : 0;
        header.level = static_cast<uint32_t>(options.level);
        header.file_size = footer.file_size;
        header.entryCount = records.size();
        header.blockCount = index.size();
        header.dataOffset = sizeof(SSTFileHeaderV2);
        header.indexOffset = indexOffset;
        header.indexSize = index.size() * sizeof(SSTBlockIndexEntryV2);
        header.keyArenaOffset = keyArenaOffset;
        header.keyArenaSize = keyArena.size();
        header.bloomOffset = bloomOffset;
        header.bloomSize = sizeof(SSTBloomHeaderV2) + bloom.bits.size();
        header.footerOffset = footerOffset;
        header.minSeq = result.minSeq;
        header.maxSeq = result.maxSeq;
        header.blockSize = options.blockSize;
        header.crc32c = 0;
        header.crc32c = cpu::CRC32C(reinterpret_cast<const std::byte*>(&header), sizeof(header));

        footer.headerCrc32c = header.crc32c;
        footer.footerCrc32c = 0;
        footer.footerCrc32c = cpu::CRC32C(reinterpret_cast<const std::byte*>(&footer), sizeof(footer));
        writeExact(out, &footer, sizeof(footer));

        out.seekp(0);
        writeExact(out, &header, sizeof(header));
        out.close();
        if (!out) { throw std::runtime_error("SSTWriter: close failed"); }

        result.fileSizeBytes = header.file_size;
        return result;
    }
} // namespace akkaradb::engine::sst

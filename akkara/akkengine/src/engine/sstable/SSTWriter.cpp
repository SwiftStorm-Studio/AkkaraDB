/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/sstable/SSTWriter.cpp
#include "akk/engine/sstable/SSTWriter.hpp"

#include <algorithm>
#include <bit>
#include <cstring>
#include <fstream>
#include <ranges>
#include <stdexcept>
#include <string>

#include <zstd.h>

#include "akk/core/record/SSTHdr32.hpp"
#include "akk/cpu/CRC32C.hpp"
#include "akk/engine/blob/BlobFraming.hpp"

namespace akkaradb::engine::sst {
    namespace {
        [[nodiscard]] uint32_t crc32c(std::span<const uint8_t> bytes) noexcept {
            const auto data = std::as_bytes(bytes);
            return cpu::CRC32C(data.data(), data.size());
        }

        [[nodiscard]] SSTWriter::Result::BlobRefEntry blobRefEntryFromRecord(const core::RecordView& rec) {
            SSTWriter::Result::BlobRefEntry entry;
            const auto key = rec.key();
            entry.key.assign_range(key);
            entry.seq = rec.seq();
            entry.flags = rec.flags();
            if ((rec.flags() & SST_RECORD_FLAG_BLOB) != 0 && rec.valueSize() >= blob::BLOB_REF_SIZE) {
                entry.blobId = blob::decodeBlobRef(rec.value().data()).blobId;
            }
            return entry;
        }

        template <typename T>
        [[nodiscard]] uint32_t crc32cPodVector(const std::vector<T>& values) noexcept {
            const auto bytes = std::as_bytes(std::span{values});
            return cpu::CRC32C(bytes.data(), bytes.size());
        }

        template <typename T>
        void appendPod(std::vector<uint8_t>& out, const T& value) {
            const auto* p = reinterpret_cast<const uint8_t*>(&value);
            out.append_range(std::span{p, sizeof(T)});
        }

        [[nodiscard]] size_t sharedPrefixLen(std::span<const uint8_t> a, std::span<const uint8_t> b) noexcept {
            if (a.empty()) { return 0; }
            const auto mismatch = std::ranges::mismatch(a, b);
            return static_cast<size_t>(mismatch.in1 - a.begin());
        }

        void writeExact(std::ofstream& out, const void* data, size_t size) {
            out.write(static_cast<const char*>(data), static_cast<std::streamsize>(size));
            if (!out) { throw std::runtime_error("SSTWriter: write failed"); }
        }

        [[nodiscard]] uint32_t nextPow2U32(uint64_t value) noexcept {
            if (value <= 2) { return 2; }
            if (value > (1ULL << 31)) { return 1u << 31; }
            return static_cast<uint32_t>(std::bit_ceil(value));
        }

        struct BloomBuild {
            SSTBloomHeaderV1 header{};
            std::vector<uint8_t> bits;

            explicit BloomBuild(size_t entries, uint32_t bitsPerKey) {
                const auto requestedBits = std::ranges::max(uint64_t{512}, static_cast<uint64_t>(entries) * std::ranges::max(1u, bitsPerKey));
                header.numBits = nextPow2U32(requestedBits);
                header.numHashes = std::ranges::max(1u, static_cast<uint32_t>(bitsPerKey * 0.69));
                header.bitsSize = header.numBits / 8;
                bits.assign(header.bitsSize, 0);
            }

            void add(uint64_t fp64) {
                const auto mask = header.numBits - 1u;
                const auto h1 = static_cast<uint32_t>(fp64);
                const auto h2 = static_cast<uint32_t>(fp64 >> 32) | 1u;
                for (uint32_t i = 0; i < header.numHashes; ++i) {
                    const auto bit = (h1 + i * h2) & mask;
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
            const auto off = static_cast<uint32_t>(arena.size());
            arena.append_range(key);
            return off;
        }

        void appendRecord(PendingBlock& block, const core::RecordView& rec) {
            if (block.raw.size() > UINT32_MAX) { throw std::runtime_error("SSTWriter: block too large"); }
            block.offsets.push_back(static_cast<uint32_t>(block.raw.size()));

            const auto key = rec.key();
            const auto value = rec.value();
            const auto hdr = core::SSTHdr32{
                .seq = rec.seq(),
                .kLen = rec.keySize(),
                .vLen = rec.valueSize(),
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
            block.raw.append_range(key);
            block.raw.append_range(value);
            const auto padded = alignUpU64(block.raw.size(), 8);
            block.raw.resize(static_cast<size_t>(padded), 0);

            if (block.offsets.size() == 1) {
                block.firstKey.assign_range(key);
                block.firstSeq = hdr.seq;
                block.firstFp = hdr.keyFp64;
                block.firstMini = hdr.miniKey;
            }
            block.lastKey.assign_range(key);
            block.lastSeq = hdr.seq;
            block.lastFp = hdr.keyFp64;
            block.lastMini = hdr.miniKey;
        }

        [[nodiscard]] std::vector<uint8_t> compressOrRaw(
            const std::vector<uint8_t>& raw,
            SSTWriter::Codec codec,
            int zstdCompressionLevel,
            uint32_t& flags
        ) {
            const auto extraFlags = flags & ~SST_BLOCK_FLAG_RAW;
            flags = extraFlags | SST_BLOCK_FLAG_RAW;
            if (codec != SSTWriter::Codec::ZSTD || raw.empty()) { return raw; }

            const auto bound = ZSTD_compressBound(raw.size());
            std::vector<uint8_t> compressed(bound);
            const auto n = ZSTD_compress(compressed.data(), compressed.size(), raw.data(), raw.size(), zstdCompressionLevel);
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
            for (const auto off : block.offsets) {
                if (off + sizeof(core::SSTHdr32) > block.raw.size()) { throw std::runtime_error("SSTWriter: corrupt source block header"); }
                const auto* hdr = reinterpret_cast<const core::SSTHdr32*>(block.raw.data() + off);
                const auto* keyPtr = block.raw.data() + off + sizeof(core::SSTHdr32);
                const auto* valPtr = keyPtr + hdr->kLen;
                if (valPtr + hdr->vLen > block.raw.data() + block.raw.size()) {
                    throw std::runtime_error("SSTWriter: corrupt source block payload");
                }

                const auto key = std::span{keyPtr, hdr->kLen};
                const auto shared = static_cast<uint16_t>(std::ranges::min(sharedPrefixLen(prevKey, key), size_t{UINT16_MAX}));

                out.offsets.push_back(static_cast<uint32_t>(out.data.size()));

                auto encHdr = *hdr;
                encHdr.reserved0 = 0;
                encHdr.reserved1 = shared;
                appendPod(out.data, encHdr);
                out.data.append_range(key.subspan(shared));
                out.data.append_range(std::span{valPtr, hdr->vLen});
                const auto padded = alignUpU64(out.data.size(), 8);
                out.data.resize(static_cast<size_t>(padded), 0);

                prevKey.assign_range(key);
            }

            return out;
        }
    } // namespace

    template <typename Records>
    [[nodiscard]] SSTWriter::Result writeRecords(
        const std::filesystem::path& path,
        size_t estimatedRecordCount,
        Records&& records,
        const SSTWriter::Options& options
    ) {
        if (options.blockSize < 4096 || (options.blockSize & 7u) != 0) {
            throw std::invalid_argument("SSTWriter::write: blockSize must be >=4096 and 8-byte aligned");
        }
        if (options.codec == SSTWriter::Codec::ZSTD && (options.zstdCompressionLevel < ZSTD_minCLevel() || options.zstdCompressionLevel >
            ZSTD_maxCLevel())) {
            throw std::invalid_argument("SSTWriter::write: zstdCompressionLevel is outside the supported Zstd range");
        }

        if (path.has_parent_path()) { std::filesystem::create_directories(path.parent_path()); }
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        if (!out) { throw std::runtime_error("SSTWriter: cannot open " + path.string()); }

        SSTFileHeaderV1 header{};
        writeExact(out, &header, sizeof(header));

        SSTWriter::Result result;
        result.path = path;

        BloomBuild bloom(estimatedRecordCount, options.bloomBitsPerKey);

        std::vector<SSTBlockIndexEntryV1> index;
        std::vector<uint8_t> keyArena;
        PendingBlock block;

        auto flushBlock = [&]() {
            if (block.empty()) { return; }

            const auto encoded = encodePrefixCompressedBlock(block);
            const auto usePrefixCompressed = !encoded.data.empty() && encoded.data.size() < block.raw.size();
            const auto& rawPayload = usePrefixCompressed ? encoded.data : block.raw;
            const auto& rawOffsets = usePrefixCompressed ? encoded.offsets : block.offsets;

            uint32_t blockFlags = usePrefixCompressed ? SST_BLOCK_FLAG_PREFIX_COMPRESSED : 0;
            const auto payload = compressOrRaw(rawPayload, options.codec, options.zstdCompressionLevel, blockFlags);
            std::vector<uint8_t> offsetsBytes;
            offsetsBytes.reserve(rawOffsets.size() * sizeof(uint32_t));
            for (const auto off : rawOffsets) { appendPod(offsetsBytes, off); }

            const auto blockOffset = static_cast<uint64_t>(out.tellp());
            SSTBlockHeaderV1 bh{};
            bh.headerSize = sizeof(SSTBlockHeaderV1);
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
            crcInput.append_range(payload);
            crcInput.append_range(offsetsBytes);
            bh.crc32c = crc32c(crcInput);

            writeExact(out, &bh, sizeof(bh));
            writeExact(out, payload.data(), payload.size());
            writeExact(out, offsetsBytes.data(), offsetsBytes.size());

            const auto next = alignUpU64(static_cast<uint64_t>(out.tellp()), 8);
            const auto pad = static_cast<size_t>(next - static_cast<uint64_t>(out.tellp()));
            if (pad > 0) {
                const uint8_t zeros[8]{};
                writeExact(out, zeros, pad);
            }

            const auto firstKeyOff = addKey(keyArena, block.firstKey);
            const auto lastKeyOff = addKey(keyArena, block.lastKey);
            index.push_back(
                SSTBlockIndexEntryV1{
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
            // lastKey owns the preceding key even after a streaming producer
            // reuses or destroys its previous record's buffers.
            if (result.entryCount != 0 && rec.compareKey(result.lastKey) < 0) {
                throw std::invalid_argument("SSTWriter::write: records must be sorted by key");
            }
            if (result.entryCount == 0) { result.firstKey.assign_range(rec.key()); }

            bloom.add(rec.keyFp64());
            result.minSeq = std::ranges::min(result.minSeq, rec.seq());
            result.maxSeq = std::ranges::max(result.maxSeq, rec.seq());
            result.lastKey.assign_range(rec.key());
            if (options.trackBlobReferences) { result.blobRefs.push_back(blobRefEntryFromRecord(rec)); }
            else if ((rec.flags() & SST_RECORD_FLAG_BLOB) != 0) {
                throw std::invalid_argument("SSTWriter: disabled Blob tracking requires Blob-free records");
            }
            ++result.entryCount;

            const auto estimated = alignUpU64(32 + rec.keySize() + rec.valueSize(), 8);
            if (!block.empty() && block.raw.size() + estimated > options.blockSize) { flushBlock(); }
            appendRecord(block, rec);
        }
        if (result.entryCount == 0) { throw std::invalid_argument("SSTWriter::write: records must be non-empty"); }
        flushBlock();

        const auto indexOffset = static_cast<uint64_t>(out.tellp());
        for (const auto& entry : index) { writeExact(out, &entry, sizeof(entry)); }

        const auto keyArenaOffset = static_cast<uint64_t>(out.tellp());
        if (!keyArena.empty()) { writeExact(out, keyArena.data(), keyArena.size()); }

        const auto bloomOffset = static_cast<uint64_t>(out.tellp());
        writeExact(out, &bloom.header, sizeof(bloom.header));
        writeExact(out, bloom.bits.data(), bloom.bits.size());

        const auto footerOffset = static_cast<uint64_t>(out.tellp());
        SSTFooterV1 footer{};
        footer.file_size = footerOffset + sizeof(SSTFooterV1);
        footer.indexOffset = indexOffset;
        footer.keyArenaOffset = keyArenaOffset;
        footer.bloomOffset = bloomOffset;
        footer.magic = SST_FOOTER_MAGIC_V1;
        footer.version = SST_VERSION_V1;

        header.magic = SST_MAGIC_V1;
        header.version = SST_VERSION_V1;
        header.headerSize = sizeof(SSTFileHeaderV1);
        header.flags = SST_FILE_FLAG_METADATA_CRC;
        if (options.codec == SSTWriter::Codec::ZSTD) { header.flags |= SST_FILE_FLAG_BLOCK_ZSTD; }
        header.level = static_cast<uint32_t>(options.level);
        header.file_size = footer.file_size;
        header.entryCount = result.entryCount;
        header.blockCount = index.size();
        header.dataOffset = sizeof(SSTFileHeaderV1);
        header.indexOffset = indexOffset;
        header.indexSize = index.size() * sizeof(SSTBlockIndexEntryV1);
        header.keyArenaOffset = keyArenaOffset;
        header.keyArenaSize = keyArena.size();
        header.bloomOffset = bloomOffset;
        header.bloomSize = sizeof(SSTBloomHeaderV1) + bloom.bits.size();
        header.footerOffset = footerOffset;
        header.minSeq = result.minSeq;
        header.maxSeq = result.maxSeq;
        header.blockSize = options.blockSize;
        std::vector<uint8_t> bloomData;
        bloomData.reserve(sizeof(bloom.header) + bloom.bits.size());
        appendPod(bloomData, bloom.header);
        bloomData.append_range(bloom.bits);
        const SSTMetadataCrcV1 metadataCrc{
            .indexCrc32c = crc32cPodVector(index),
            .keyArenaCrc32c = crc32c(keyArena),
            .bloomCrc32c = crc32c(bloomData)
        };
        std::memcpy(header.reserved, &metadataCrc, sizeof(metadataCrc));
        header.crc32c = 0;
        const auto headerBytes = std::as_bytes(std::span{&header, 1});
        header.crc32c = cpu::CRC32C(headerBytes.data(), headerBytes.size());

        footer.headerCrc32c = header.crc32c;
        footer.footerCrc32c = 0;
        const auto footerBytes = std::as_bytes(std::span{&footer, 1});
        footer.footerCrc32c = cpu::CRC32C(footerBytes.data(), footerBytes.size());
        writeExact(out, &footer, sizeof(footer));

        out.seekp(0);
        writeExact(out, &header, sizeof(header));
        out.close();
        if (!out) { throw std::runtime_error("SSTWriter: close failed"); }

        result.fileSizeBytes = header.file_size;
        return result;
    }

    SSTWriter::Result SSTWriter::write(const std::filesystem::path& path, std::span<const core::RecordView> records) {
        return write(path, records, Options{});
    }

    SSTWriter::Result SSTWriter::write(
        const std::filesystem::path& path,
        std::span<const core::RecordView> records,
        const Options& options
    ) {
        if (records.empty()) { throw std::invalid_argument("SSTWriter::write: records must be non-empty"); }
        return writeRecords(path, records.size(), records, options);
    }

    SSTWriter::Result SSTWriter::write(
        const std::filesystem::path& path,
        size_t estimatedRecordCount,
        core::ArenaGenerator<core::RecordView> records,
        const Options& options
    ) { return writeRecords(path, estimatedRecordCount, records, options); }
} // namespace akkaradb::engine::sst

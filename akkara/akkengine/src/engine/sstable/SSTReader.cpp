/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/sstable/SSTReader.cpp
#include "akk/engine/sstable/SSTReader.hpp"

#include <algorithm>
#include <cstring>
#include <fstream>
#include <list>
#include <limits>
#include <mutex>
#include <ranges>
#include <stdexcept>
#include <unordered_map>

#include <zstd.h>

#include "akk/cpu/CRC32C.hpp"
#include "akk/core/record/KeyFingerprint.hpp"

namespace akkaradb::engine::sst {
    namespace {
        [[nodiscard]] uint32_t crc32c(std::span<const uint8_t> bytes) noexcept {
            const auto data = std::as_bytes(bytes);
            return cpu::CRC32C(data.data(), data.size());
        }

        template <typename T>
        [[nodiscard]] uint32_t crc32cPodVector(const std::vector<T>& values) noexcept {
            const auto bytes = std::as_bytes(std::span{values});
            return cpu::CRC32C(bytes.data(), bytes.size());
        }

        [[nodiscard]] bool rangeWithin(uint64_t offset, uint64_t size, uint64_t limit) noexcept {
            return offset <= limit && size <= limit - offset;
        }

        template <typename T>
        void appendPod(std::vector<uint8_t>& out, const T& value) {
            const auto* p = reinterpret_cast<const uint8_t*>(&value);
            out.append_range(std::span{p, sizeof(T)});
        }

        [[nodiscard]] int compareBytes(std::span<const uint8_t> a, std::span<const uint8_t> b) noexcept {
            const auto n = std::ranges::min(a.size(), b.size());
            if (n > 0) {
                const auto c = std::memcmp(a.data(), b.data(), n);
                if (c != 0) { return c < 0 ? -1 : 1; }
            }
            if (a.size() < b.size()) { return -1; }
            if (a.size() > b.size()) { return 1; }
            return 0;
        }

        [[nodiscard]] bool keyLess(std::span<const uint8_t> a, std::span<const uint8_t> b) noexcept {
            return compareBytes(a, b) < 0;
        }

        [[nodiscard]] bool keyEquals(
            const core::SSTHdr32& hdr,
            const uint8_t* keyData,
            std::span<const uint8_t> target,
            uint64_t targetFp,
            uint64_t targetMini
        ) noexcept {
            if (hdr.kLen != target.size()) { return false; }
            if (hdr.keyFp64 != targetFp || hdr.miniKey != targetMini) { return false; }
            if (hdr.kLen == 0) { return true; }
            return std::ranges::equal(std::span{keyData, hdr.kLen}, target);
        }

        [[nodiscard]] int compareRecordKey(const core::SSTHdr32& hdr, const uint8_t* keyData, std::span<const uint8_t> target) noexcept {
            return compareBytes({keyData, hdr.kLen}, target);
        }

        [[nodiscard]] std::span<const uint8_t> arenaKey(const std::vector<uint8_t>& arena, uint32_t off, uint32_t len) noexcept {
            if (off > arena.size() || len > arena.size() - off) { return {}; }
            return {arena.data() + off, len};
        }

        struct DecodedBlockData {
            std::vector<uint8_t> data;
            std::vector<uint32_t> offsets;
        };

        [[nodiscard]] std::optional<DecodedBlockData> decodePrefixCompressedBlock(
            std::span<const uint8_t> encoded,
            std::span<const uint32_t> encodedOffsets
        ) {
            DecodedBlockData out;
            out.offsets.reserve(encodedOffsets.size());

            std::vector<uint8_t> prevKey;
            for (const auto off : encodedOffsets) {
                if (off + sizeof(core::SSTHdr32) > encoded.size()) { return std::nullopt; }

                const auto* hdr = reinterpret_cast<const core::SSTHdr32*>(encoded.data() + off);
                const auto shared = hdr->reserved1;
                if (shared > hdr->kLen || shared > prevKey.size()) { return std::nullopt; }

                const auto* suffixPtr = encoded.data() + off + sizeof(core::SSTHdr32);
                const auto suffixLen = static_cast<size_t>(hdr->kLen) - shared;
                const auto* valPtr = suffixPtr + suffixLen;
                if (valPtr + hdr->vLen > encoded.data() + encoded.size()) { return std::nullopt; }

                std::vector<uint8_t> fullKey;
                fullKey.reserve(hdr->kLen);
                fullKey.append_range(std::span{prevKey}.first(shared));
                fullKey.append_range(std::span{suffixPtr, suffixLen});

                out.offsets.push_back(static_cast<uint32_t>(out.data.size()));
                auto decodedHdr = *hdr;
                decodedHdr.reserved0 = 0;
                decodedHdr.reserved1 = 0;
                appendPod(out.data, decodedHdr);
                out.data.append_range(fullKey);
                out.data.append_range(std::span{valPtr, hdr->vLen});
                const auto padded = alignUpU64(out.data.size(), 8);
                out.data.resize(static_cast<size_t>(padded), 0);

                prevKey = std::move(fullKey);
            }

            return out;
        }
    } // namespace

    class SSTReader::Impl {
        public:
            struct Block {
                std::vector<uint8_t> data;
                std::vector<uint32_t> offsets;
                size_t bytes = 0;
            };

            struct RecordLocation {
                std::shared_ptr<Block> block;
                uint32_t offset;

                [[nodiscard]] const core::SSTHdr32& header() const noexcept {
                    return *reinterpret_cast<const core::SSTHdr32*>(block->data.data() + offset);
                }
                [[nodiscard]] const uint8_t* keyData() const noexcept {
                    return block->data.data() + offset + sizeof(core::SSTHdr32);
                }
            };

            struct CacheEntry {
                size_t blockIndex;
                std::shared_ptr<Block> block;
            };

            Impl(std::filesystem::path path, Options options) : path_{std::move(path)}, options_{options} {}

            [[nodiscard]] bool open() {
                try {
                    file_.open(path_, std::ios::binary);
                    if (!file_) { return false; }

                    readAt(0, &header_, sizeof(header_));
                    if (header_.magic != SST_MAGIC_V1 || header_.version != SST_VERSION_V1 || header_.headerSize != sizeof(
                        SSTFileHeaderV1)) { return false; }
                    const auto stored = header_.crc32c;
                    header_.crc32c = 0;
                    const auto headerBytes = std::as_bytes(std::span{&header_, 1});
                    const auto computed = cpu::CRC32C(headerBytes.data(), headerBytes.size());
                    header_.crc32c = stored;
                    if (stored != computed) { return false; }
                    if ((header_.flags & SST_FILE_FLAG_METADATA_CRC) == 0) { return false; }
                    const auto actualFileSize = std::filesystem::file_size(path_);
                    if (actualFileSize != header_.file_size || header_.file_size < sizeof(SSTFileHeaderV1) + sizeof(SSTFooterV1)) {
                        return false;
                    }
                    if (header_.dataOffset != sizeof(SSTFileHeaderV1) || header_.entryCount == 0 || header_.blockCount == 0 || header_.
                        footerOffset != header_.file_size - sizeof(SSTFooterV1) || header_.blockCount > UINT64_MAX / sizeof(
                            SSTBlockIndexEntryV1) || header_.indexSize != header_.blockCount * sizeof(SSTBlockIndexEntryV1) || header_.
                        blockCount > std::numeric_limits<size_t>::max() || header_.keyArenaSize > std::numeric_limits<size_t>::max() ||
                        header_.bloomSize > std::numeric_limits<size_t>::max() || header_.indexOffset <= header_.dataOffset || header_.
                        keyArenaOffset < header_.indexOffset || header_.bloomOffset < header_.keyArenaOffset || header_.footerOffset <=
                        header_.bloomOffset || !rangeWithin(header_.indexOffset, header_.indexSize, header_.keyArenaOffset) || !rangeWithin(
                            header_.keyArenaOffset,
                            header_.keyArenaSize,
                            header_.bloomOffset
                        ) || !rangeWithin(header_.bloomOffset, header_.bloomSize, header_.footerOffset)) { return false; }

                    SSTFooterV1 footer{};
                    readAt(header_.footerOffset, &footer, sizeof(footer));
                    const auto storedFooter = footer.footerCrc32c;
                    footer.footerCrc32c = 0;
                    const auto footerBytes = std::as_bytes(std::span{&footer, 1});
                    const auto computedFooter = cpu::CRC32C(footerBytes.data(), footerBytes.size());
                    if (storedFooter != computedFooter || footer.magic != SST_FOOTER_MAGIC_V1 || footer.version != SST_VERSION_V1 || footer.
                        headerCrc32c != stored || footer.file_size != header_.file_size || footer.indexOffset != header_.indexOffset ||
                        footer.keyArenaOffset != header_.keyArenaOffset || footer.bloomOffset != header_.bloomOffset) { return false; }

                    index_.resize(static_cast<size_t>(header_.blockCount));
                    if (!index_.empty()) { readAt(header_.indexOffset, index_.data(), index_.size() * sizeof(SSTBlockIndexEntryV1)); }
                    keyArena_ = readVecAt(header_.keyArenaOffset, static_cast<size_t>(header_.keyArenaSize));
                    bloomData_ = readVecAt(header_.bloomOffset, static_cast<size_t>(header_.bloomSize));
                    SSTMetadataCrcV1 metadataCrc{};
                    std::memcpy(&metadataCrc, header_.reserved, sizeof(metadataCrc));
                    if (crc32cPodVector(index_) != metadataCrc.indexCrc32c || crc32c(keyArena_) != metadataCrc.keyArenaCrc32c || crc32c(
                        bloomData_
                    ) != metadataCrc.bloomCrc32c) { return false; }
                    if (bloomData_.size() < sizeof(SSTBloomHeaderV1)) { return false; }
                    std::memcpy(&bloomHeader_, bloomData_.data(), sizeof(bloomHeader_));
                    if (bloomHeader_.numBits == 0 || bloomHeader_.numHashes == 0 || (bloomHeader_.numBits & (bloomHeader_.numBits - 1u)) !=
                        0 || bloomHeader_.bitsSize != bloomHeader_.numBits / 8u || bloomHeader_.bitsSize + sizeof(SSTBloomHeaderV1) !=
                        bloomData_.size() || !validateIndexMetadata()) { return false; }
                    const auto first = arenaKey(keyArena_, index_.front().firstKeyOffset, index_.front().firstKeyLen);
                    const auto last = arenaKey(keyArena_, index_.back().lastKeyOffset, index_.back().lastKeyLen);
                    firstKey_.assign_range(first);
                    lastKey_.assign_range(last);
                    cacheCapacity_ = options_.blockCacheBytes;
                    return true;
                }
                catch (...) { return false; }
            }

            [[nodiscard]] bool keyInRange(std::span<const uint8_t> key) const noexcept {
                if (index_.empty()) { return false; }
                return compareBytes(firstKey_, key) <= 0 && compareBytes(key, lastKey_) <= 0;
            }

            [[nodiscard]] std::optional<SSTRecord> get(std::span<const uint8_t> key, uint64_t snapshotSeq) const {
                const auto location = findRecord(key, snapshotSeq);
                if (!location) { return std::nullopt; }
                const auto& hdr = location->header();
                const auto* keyPtr = location->keyData();
                const auto* valPtr = keyPtr + hdr.kLen;
                return SSTRecord{
                    .key = std::ranges::to<std::vector>(std::span{keyPtr, hdr.kLen}),
                    .value = std::ranges::to<std::vector>(std::span{valPtr, hdr.vLen}),
                    .seq = hdr.seq,
                    .flags = hdr.flags,
                    .keyFp64 = hdr.keyFp64,
                    .miniKey = hdr.miniKey
                };
            }

            [[nodiscard]] std::optional<bool> contains(std::span<const uint8_t> key, uint64_t snapshotSeq) const {
                const auto location = findRecord(key, snapshotSeq);
                if (!location) { return std::nullopt; }
                return (location->header().flags & core::SSTHdr32::FLAG_TOMBSTONE) == 0;
            }

            [[nodiscard]] std::optional<bool> getInto(std::span<const uint8_t> key, std::vector<uint8_t>& out, uint64_t snapshotSeq) const {
                const auto location = findRecord(key, snapshotSeq);
                if (!location) { return std::nullopt; }
                const auto& hdr = location->header();
                if ((hdr.flags & core::SSTHdr32::FLAG_TOMBSTONE) != 0) { return false; }
                const auto* valPtr = location->keyData() + hdr.kLen;
                out.assign_range(std::span{valPtr, hdr.vLen});
                return true;
            }

            [[nodiscard]] core::ArenaGenerator<SSTRecord> scan(
                std::vector<uint8_t> startKey,
                std::vector<uint8_t> endKey,
                uint64_t snapshotSeq
            ) const {
                for (const auto& [blockIndex, entry] : std::views::enumerate(index_)) {
                    const auto first = arenaKey(keyArena_, entry.firstKeyOffset, entry.firstKeyLen);
                    const auto last = arenaKey(keyArena_, entry.lastKeyOffset, entry.lastKeyLen);
                    if (!endKey.empty() && compareBytes(first, endKey) >= 0) { break; }
                    if (!startKey.empty() && compareBytes(last, startKey) < 0) { continue; }

                    const auto block = loadBlock(blockIndex);
                    for (const auto off : block->offsets) {
                        if (off + sizeof(core::SSTHdr32) > block->data.size()) {
                            throw std::runtime_error("SSTReader: corrupt record offset");
                        }
                        const auto* hdr = reinterpret_cast<const core::SSTHdr32*>(block->data.data() + off);
                        const auto* keyPtr = block->data.data() + off + sizeof(core::SSTHdr32);
                        const auto* valPtr = keyPtr + hdr->kLen;
                        if (valPtr + hdr->vLen > block->data.data() + block->data.size()) {
                            throw std::runtime_error("SSTReader: corrupt record payload");
                        }
                        const auto key = std::span{keyPtr, hdr->kLen};
                        if (!startKey.empty() && compareBytes(key, startKey) < 0) { continue; }
                        if (!endKey.empty() && compareBytes(key, endKey) >= 0) { co_return; }
                        if (hdr->seq > snapshotSeq) { continue; }
                        co_yield SSTRecord{
                            .key = std::ranges::to<std::vector>(key),
                            .value = std::ranges::to<std::vector>(std::span{valPtr, hdr->vLen}),
                            .seq = hdr->seq,
                            .flags = hdr->flags,
                            .keyFp64 = hdr->keyFp64,
                            .miniKey = hdr->miniKey
                        };
                    }
                }
            }

            [[nodiscard]] const SSTFileHeaderV1& header() const noexcept { return header_; }
            [[nodiscard]] std::span<const uint8_t> firstKey() const noexcept { return firstKey_; }
            [[nodiscard]] std::span<const uint8_t> lastKey() const noexcept { return lastKey_; }
            [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

        private:
            [[nodiscard]] bool validateIndexMetadata() const noexcept {
                uint64_t expectedBlockOffset = header_.dataOffset;
                std::span<const uint8_t> previousLast;
                bool havePrevious = false;
                for (const auto& entry : index_) {
                    const auto arenaRangeIsValid = [this](uint32_t offset, uint32_t length) noexcept {
                        return offset <= keyArena_.size() && length <= keyArena_.size() - offset;
                    };
                    if (entry.blockOffset != expectedBlockOffset || entry.blockSize < sizeof(SSTBlockHeaderV1) || entry.recordCount == 0 ||
                        entry.uncompressedSize == 0 || !rangeWithin(entry.blockOffset, entry.blockSize, header_.indexOffset) || !
                        arenaRangeIsValid(entry.firstKeyOffset, entry.firstKeyLen) || !arenaRangeIsValid(
                            entry.lastKeyOffset,
                            entry.lastKeyLen
                        )) { return false; }
                    const auto first = arenaKey(keyArena_, entry.firstKeyOffset, entry.firstKeyLen);
                    const auto last = arenaKey(keyArena_, entry.lastKeyOffset, entry.lastKeyLen);
                    if (compareBytes(first, last) > 0 || (havePrevious && compareBytes(previousLast, first) > 0)) { return false; }
                    previousLast = last;
                    havePrevious = true;
                    expectedBlockOffset = entry.blockOffset + entry.blockSize;
                }
                return expectedBlockOffset == header_.indexOffset;
            }

            void readAt(uint64_t offset, void* data, size_t size) const {
                std::lock_guard lock{ioMu_};
                file_.clear();
                file_.seekg(static_cast<std::streamoff>(offset));
                file_.read(static_cast<char*>(data), static_cast<std::streamsize>(size));
                if (!file_ || file_.gcount() != static_cast<std::streamsize>(size)) { throw std::runtime_error("SSTReader: short read"); }
            }

            [[nodiscard]] std::vector<uint8_t> readVecAt(uint64_t offset, size_t size) const {
                std::vector<uint8_t> out(size);
                if (size > 0) { readAt(offset, out.data(), size); }
                return out;
            }

            [[nodiscard]] bool bloomMightContain(uint64_t fp) const noexcept {
                if (bloomHeader_.numBits == 0 || bloomHeader_.bitsSize == 0) { return true; }
                const auto* bits = bloomData_.data() + sizeof(SSTBloomHeaderV1);
                const auto mask = bloomHeader_.numBits - 1u;
                const auto h1 = static_cast<uint32_t>(fp);
                const auto h2 = static_cast<uint32_t>(fp >> 32) | 1u;
                for (uint32_t i = 0; i < bloomHeader_.numHashes; ++i) {
                    const auto bit = (h1 + i * h2) & mask;
                    if ((bits[bit >> 3] & (1u << (bit & 7u))) == 0) { return false; }
                }
                return true;
            }

            [[nodiscard]] std::optional<size_t> candidateBlock(std::span<const uint8_t> key) const noexcept {
                const auto candidate = std::ranges::lower_bound(index_, key, keyLess, [this](const auto& entry) {
                    return arenaKey(keyArena_, entry.lastKeyOffset, entry.lastKeyLen);
                });
                if (candidate == index_.end()) { return std::nullopt; }
                const auto first = arenaKey(keyArena_, candidate->firstKeyOffset, candidate->firstKeyLen);
                const auto last = arenaKey(keyArena_, candidate->lastKeyOffset, candidate->lastKeyLen);
                if (compareBytes(first, key) <= 0 && compareBytes(key, last) <= 0) {
                    return static_cast<size_t>(candidate - index_.begin());
                }
                return std::nullopt;
            }

            void validateDecodedBlock(const Block& block, uint32_t recordCount) const {
                if (block.offsets.size() != recordCount) { throw std::runtime_error("SSTReader: corrupt block offset count"); }
                for (size_t i = 0; i < block.offsets.size(); ++i) {
                    const size_t offset = block.offsets[i];
                    if (offset > block.data.size() || sizeof(core::SSTHdr32) > block.data.size() - offset) {
                        throw std::runtime_error("SSTReader: corrupt record offset");
                    }
                    const auto* hdr = reinterpret_cast<const core::SSTHdr32*>(block.data.data() + offset);
                    const auto payloadOffset = offset + sizeof(core::SSTHdr32);
                    const auto payloadSize = static_cast<size_t>(hdr->kLen) + hdr->vLen;
                    if (payloadOffset > block.data.size() || payloadSize > block.data.size() - payloadOffset) {
                        throw std::runtime_error("SSTReader: corrupt record payload");
                    }
                    const auto nextOffset = static_cast<size_t>(alignUpU64(payloadOffset + payloadSize, 8));
                    if (nextOffset > block.data.size() || (i + 1 < block.offsets.size() && nextOffset != block.offsets[i + 1]) || (i + 1 ==
                        block.offsets.size() && nextOffset != block.data.size())) {
                        throw std::runtime_error("SSTReader: corrupt record layout");
                    }
                }
            }

            [[nodiscard]] std::shared_ptr<Block> loadBlock(size_t idx) const {
                {
                    std::lock_guard lock{cacheMu_};
                    auto it = cacheMap_.find(idx);
                    if (it != cacheMap_.end()) {
                        cacheLru_.splice(cacheLru_.begin(), cacheLru_, it->second);
                        return it->second->block;
                    }
                }

                if (idx >= index_.size()) { throw std::out_of_range("SSTReader: block index out of range"); }
                const auto& entry = index_[idx];
                const auto blockFile = readVecAt(entry.blockOffset, entry.blockSize);
                if (blockFile.size() < sizeof(SSTBlockHeaderV1)) { throw std::runtime_error("SSTReader: corrupt block header"); }

                SSTBlockHeaderV1 bh{};
                std::memcpy(&bh, blockFile.data(), sizeof(bh));
                constexpr auto knownBlockFlags = SST_BLOCK_FLAG_COMPRESSED | SST_BLOCK_FLAG_RAW | SST_BLOCK_FLAG_PREFIX_COMPRESSED;
                const auto compressed = (bh.flags & SST_BLOCK_FLAG_COMPRESSED) != 0;
                const auto raw = (bh.flags & SST_BLOCK_FLAG_RAW) != 0;
                if (bh.headerSize != sizeof(SSTBlockHeaderV1) || bh.recordCount != entry.recordCount || bh.flags != entry.flags || (bh.flags
                    & ~knownBlockFlags) != 0 || compressed == raw || (compressed && (header_.flags & SST_FILE_FLAG_BLOCK_ZSTD) == 0)) {
                    throw std::runtime_error("SSTReader: corrupt block metadata");
                }

                const auto payloadOff = sizeof(SSTBlockHeaderV1);
                const auto offsetsOff = payloadOff + bh.compressedSize;
                const auto crcLen = static_cast<size_t>(bh.compressedSize) + bh.offsetsSize;
                const auto expectedBlockSize = static_cast<size_t>(alignUpU64(payloadOff + crcLen, 8));
                if (offsetsOff > blockFile.size() || bh.offsetsSize > blockFile.size() - offsetsOff || payloadOff > blockFile.size() ||
                    crcLen > blockFile.size() - payloadOff || expectedBlockSize != blockFile.size()) {
                    throw std::runtime_error("SSTReader: corrupt block bounds");
                }

                const auto payload = std::span{blockFile.data() + payloadOff, bh.compressedSize};
                const auto offsetsBytes = std::span{blockFile.data() + offsetsOff, bh.offsetsSize};
                if (crc32c({blockFile.data() + payloadOff, crcLen}) != bh.crc32c) {
                    throw std::runtime_error("SSTReader: block CRC mismatch");
                }

                auto block = std::make_shared<Block>();
                if (compressed) {
                    block->data.resize(bh.uncompressedSize);
                    const auto n = ZSTD_decompress(block->data.data(), block->data.size(), payload.data(), payload.size());
                    if (ZSTD_isError(n) || n != bh.uncompressedSize) { throw std::runtime_error("SSTReader: corrupt Zstd block payload"); }
                }
                else {
                    block->data.assign_range(payload);
                    if (block->data.size() != bh.uncompressedSize) { throw std::runtime_error("SSTReader: corrupt raw block size"); }
                }
                if (offsetsBytes.size() % sizeof(uint32_t) != 0) { throw std::runtime_error("SSTReader: corrupt block offsets"); }
                block->offsets.resize(offsetsBytes.size() / sizeof(uint32_t));
                if (!block->offsets.empty()) { std::memcpy(block->offsets.data(), offsetsBytes.data(), offsetsBytes.size()); }
                if ((bh.flags & SST_BLOCK_FLAG_PREFIX_COMPRESSED) != 0) {
                    auto decoded = decodePrefixCompressedBlock(block->data, block->offsets);
                    if (!decoded.has_value()) { throw std::runtime_error("SSTReader: corrupt prefix-compressed block"); }
                    block->data = std::move(decoded->data);
                    block->offsets = std::move(decoded->offsets);
                }
                validateDecodedBlock(*block, bh.recordCount);
                block->bytes = block->data.size() + block->offsets.size() * sizeof(uint32_t);
                putCache(idx, block);
                return block;
            }

            void putCache(size_t idx, const std::shared_ptr<Block>& block) const {
                if (cacheCapacity_ == 0 || !block) { return; }
                std::lock_guard lock{cacheMu_};
                cacheLru_.push_front(CacheEntry{.blockIndex = idx, .block = block});
                cacheMap_[idx] = cacheLru_.begin();
                cacheBytes_ += block->bytes;
                while (cacheBytes_ > cacheCapacity_ && !cacheLru_.empty()) {
                    const auto& [blockIndex, block] = cacheLru_.back();
                    cacheBytes_ -= block ? block->bytes : 0;
                    cacheMap_.erase(blockIndex);
                    cacheLru_.pop_back();
                }
            }

            [[nodiscard]] std::optional<RecordLocation> findRecord(std::span<const uint8_t> key, uint64_t snapshotSeq) const {
                if (!keyInRange(key)) { return std::nullopt; }
                const auto fp = core::computeKeyFp64(key);
                const auto mini = core::buildMiniKey(key);
                if (!bloomMightContain(fp)) { return std::nullopt; }
                const auto firstBlock = candidateBlock(key);
                if (!firstBlock) { return std::nullopt; }
                std::optional<RecordLocation> best;
                // Equal keys may span blocks, and their sequences need not be ordered.
                for (const auto& [i, entry] : std::views::enumerate(index_) | std::views::drop(*firstBlock)) {
                    const auto first = arenaKey(keyArena_, entry.firstKeyOffset, entry.firstKeyLen);
                    if (compareBytes(first, key) > 0) { break; }
                    const auto block = loadBlock(i);
                    const auto firstRecord = std::ranges::lower_bound(block->offsets, key, keyLess, [&block](auto offset) {
                        const RecordLocation candidate{.block = block, .offset = offset};
                        return std::span{candidate.keyData(), candidate.header().kLen};
                    });
                    for (const auto offset : std::ranges::subrange{firstRecord, block->offsets.end()}) {
                        const RecordLocation candidate{.block = block, .offset = offset};
                        const auto& hdr = candidate.header();
                        if (compareRecordKey(hdr, candidate.keyData(), key) != 0) { break; }
                        if (keyEquals(hdr, candidate.keyData(), key, fp, mini) && hdr.seq <= snapshotSeq &&
                            (!best || hdr.seq > best->header().seq)) {
                            best = candidate;
                        }
                    }
                }
                return best;
            }

            std::filesystem::path path_;
            Options options_;
            mutable std::ifstream file_;
            mutable std::mutex ioMu_;
            SSTFileHeaderV1 header_{};
            std::vector<SSTBlockIndexEntryV1> index_;
            std::vector<uint8_t> keyArena_;
            std::vector<uint8_t> bloomData_;
            SSTBloomHeaderV1 bloomHeader_{};
            std::vector<uint8_t> firstKey_;
            std::vector<uint8_t> lastKey_;

            mutable std::mutex cacheMu_;
            mutable std::list<CacheEntry> cacheLru_;
            mutable std::unordered_map<size_t, std::list<CacheEntry>::iterator> cacheMap_;
            mutable uint64_t cacheBytes_{0};
            uint64_t cacheCapacity_{0};
    };

    std::unique_ptr<SSTReader> SSTReader::open(const std::filesystem::path& path) { return open(path, Options{}); }

    std::unique_ptr<SSTReader> SSTReader::open(const std::filesystem::path& path, const Options& options) {
        auto reader = std::unique_ptr<SSTReader>{new SSTReader()};
        reader->impl_ = std::make_unique<Impl>(path, options);
        if (!reader->impl_->open()) { return nullptr; }
        return reader;
    }

    SSTReader::SSTReader() = default;
    SSTReader::~SSTReader() = default;
    SSTReader::SSTReader(SSTReader&&) noexcept = default;
    SSTReader& SSTReader::operator=(SSTReader&&) noexcept = default;

    std::optional<SSTRecord> SSTReader::get(std::span<const uint8_t> key, uint64_t snapshotSeq) const {
        return impl_->get(key, snapshotSeq);
    }

    std::optional<bool> SSTReader::contains(std::span<const uint8_t> key, uint64_t snapshotSeq) const {
        return impl_->contains(key, snapshotSeq);
    }

    std::optional<bool> SSTReader::getInto(std::span<const uint8_t> key, std::vector<uint8_t>& out, uint64_t snapshotSeq) const {
        return impl_->getInto(key, out, snapshotSeq);
    }

    core::ArenaGenerator<SSTRecord> SSTReader::scan(
        std::span<const uint8_t> startKey,
        std::span<const uint8_t> endKey,
        uint64_t snapshotSeq
    ) const {
        return impl_->scan(
            std::ranges::to<std::vector>(startKey),
            std::ranges::to<std::vector>(endKey),
            snapshotSeq
        );
    }

    bool SSTReader::keyInRange(std::span<const uint8_t> key) const noexcept { return impl_->keyInRange(key); }
    const SSTFileHeaderV1& SSTReader::header() const noexcept { return impl_->header(); }
    std::span<const uint8_t> SSTReader::firstKey() const noexcept { return impl_->firstKey(); }
    std::span<const uint8_t> SSTReader::lastKey() const noexcept { return impl_->lastKey(); }
    const std::filesystem::path& SSTReader::path() const noexcept { return impl_->path(); }
} // namespace akkaradb::engine::sst

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
#include <mutex>
#include <stdexcept>
#include <unordered_map>

#include <zstd.h>

#include "akk/cpu/CRC32C.hpp"
#include "akk/core/record/KeyFingerprint.hpp"

namespace akkaradb::engine::sst {
    namespace {
        void readAt(const std::filesystem::path& path, uint64_t offset, void* data, size_t size) {
            std::ifstream in(path, std::ios::binary);
            if (!in) { throw std::runtime_error("SSTReader: cannot open " + path.string()); }
            in.seekg(static_cast<std::streamoff>(offset));
            in.read(reinterpret_cast<char*>(data), static_cast<std::streamsize>(size));
            if (!in || in.gcount() != static_cast<std::streamsize>(size)) { throw std::runtime_error("SSTReader: short read"); }
        }

        [[nodiscard]] std::vector<uint8_t> readVecAt(const std::filesystem::path& path, uint64_t offset, size_t size) {
            std::vector<uint8_t> out(size);
            if (size > 0) { readAt(path, offset, out.data(), size); }
            return out;
        }

        [[nodiscard]] uint32_t crc32c(std::span<const uint8_t> bytes) noexcept {
            return cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size());
        }

        template <typename T>
        void appendPod(std::vector<uint8_t>& out, const T& value) {
            const auto* p = reinterpret_cast<const uint8_t*>(&value);
            out.insert(out.end(), p, p + sizeof(T));
        }

        [[nodiscard]] int compareBytes(std::span<const uint8_t> a, std::span<const uint8_t> b) noexcept {
            const size_t n = std::min(a.size(), b.size());
            if (n > 0) {
                const int c = std::memcmp(a.data(), b.data(), n);
                if (c != 0) { return c < 0 ? -1 : 1; }
            }
            if (a.size() < b.size()) { return -1; }
            if (a.size() > b.size()) { return 1; }
            return 0;
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
            return std::memcmp(keyData, target.data(), hdr.kLen) == 0;
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
            for (size_t i = 0; i < encodedOffsets.size(); ++i) {
                const uint32_t off = encodedOffsets[i];
                if (off + sizeof(core::SSTHdr32) > encoded.size()) { return std::nullopt; }

                const auto* hdr = reinterpret_cast<const core::SSTHdr32*>(encoded.data() + off);
                const uint16_t shared = hdr->reserved1;
                if (shared > hdr->kLen || shared > prevKey.size()) { return std::nullopt; }

                const uint8_t* suffixPtr = encoded.data() + off + sizeof(core::SSTHdr32);
                const size_t suffixLen = static_cast<size_t>(hdr->kLen) - shared;
                const uint8_t* valPtr = suffixPtr + suffixLen;
                if (valPtr + hdr->vLen > encoded.data() + encoded.size()) { return std::nullopt; }

                std::vector<uint8_t> fullKey;
                fullKey.reserve(hdr->kLen);
                fullKey.insert(fullKey.end(), prevKey.begin(), prevKey.begin() + shared);
                fullKey.insert(fullKey.end(), suffixPtr, suffixPtr + suffixLen);

                out.offsets.push_back(static_cast<uint32_t>(out.data.size()));
                core::SSTHdr32 decodedHdr = *hdr;
                decodedHdr.reserved0 = 0;
                decodedHdr.reserved1 = 0;
                appendPod(out.data, decodedHdr);
                out.data.insert(out.data.end(), fullKey.begin(), fullKey.end());
                out.data.insert(out.data.end(), valPtr, valPtr + hdr->vLen);
                const uint64_t padded = alignUpU64(static_cast<uint64_t>(out.data.size()), 8);
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
                    if (header_.magic != SST_MAGIC_V2 || header_.version != SST_VERSION_V2 || header_.headerSize != sizeof(
                        SSTFileHeaderV2)) { return false; }
                    const uint32_t stored = header_.crc32c;
                    header_.crc32c = 0;
                    const uint32_t computed = cpu::CRC32C(reinterpret_cast<const std::byte*>(&header_), sizeof(header_));
                    header_.crc32c = stored;
                    if (stored != computed) { return false; }
                    if (!std::filesystem::exists(path_) || std::filesystem::file_size(path_) < header_.file_size) { return false; }

                    SSTFooterV2 footer{};
                    readAt(header_.footerOffset, &footer, sizeof(footer));
                    const uint32_t storedFooter = footer.footerCrc32c;
                    footer.footerCrc32c = 0;
                    const uint32_t computedFooter = cpu::CRC32C(reinterpret_cast<const std::byte*>(&footer), sizeof(footer));
                    if (storedFooter != computedFooter || footer.magic != SST_FOOTER_MAGIC_V2 || footer.version != SST_VERSION_V2 || footer.
                        headerCrc32c != stored) { return false; }

                    index_.resize(static_cast<size_t>(header_.blockCount));
                    if (!index_.empty()) { readAt(header_.indexOffset, index_.data(), index_.size() * sizeof(SSTBlockIndexEntryV2)); }
                    keyArena_ = readVecAt(header_.keyArenaOffset, static_cast<size_t>(header_.keyArenaSize));
                    bloomData_ = readVecAt(header_.bloomOffset, static_cast<size_t>(header_.bloomSize));
                    if (bloomData_.size() < sizeof(SSTBloomHeaderV2)) { return false; }
                    std::memcpy(&bloomHeader_, bloomData_.data(), sizeof(bloomHeader_));
                    if (bloomHeader_.bitsSize + sizeof(SSTBloomHeaderV2) != bloomData_.size()) { return false; }
                    if (!index_.empty()) {
                        firstKey_.assign(
                            arenaKey(keyArena_, index_.front().firstKeyOffset, index_.front().firstKeyLen).begin(),
                            arenaKey(keyArena_, index_.front().firstKeyOffset, index_.front().firstKeyLen).end()
                        );
                        lastKey_.assign(
                            arenaKey(keyArena_, index_.back().lastKeyOffset, index_.back().lastKeyLen).begin(),
                            arenaKey(keyArena_, index_.back().lastKeyOffset, index_.back().lastKeyLen).end()
                        );
                    }
                    cacheCapacity_ = options_.blockCacheBytes;
                    return true;
                }
                catch (...) { return false; }
            }

            [[nodiscard]] bool keyInRange(std::span<const uint8_t> key) const noexcept {
                if (index_.empty()) { return false; }
                return compareBytes(firstKey_, key) <= 0 && compareBytes(key, lastKey_) <= 0;
            }

            [[nodiscard]] std::optional<SSTRecord> get(std::span<const uint8_t> key) const {
                if (!keyInRange(key)) { return std::nullopt; }
                const uint64_t fp = key.empty() ? 0 : core::computeKeyFp64(key.data(), key.size());
                const uint64_t mini = key.empty() ? 0 : core::buildMiniKey(key.data(), key.size());
                if (!bloomMightContain(fp)) { return std::nullopt; }
                const auto blockIndex = candidateBlock(key);
                if (!blockIndex.has_value()) { return std::nullopt; }
                const auto block = loadBlock(*blockIndex);
                if (!block) { return std::nullopt; }
                return findInBlock(*block, key, fp, mini, true);
            }

            [[nodiscard]] std::optional<bool> contains(std::span<const uint8_t> key) const {
                if (!keyInRange(key)) { return std::nullopt; }
                const uint64_t fp = key.empty() ? 0 : core::computeKeyFp64(key.data(), key.size());
                const uint64_t mini = key.empty() ? 0 : core::buildMiniKey(key.data(), key.size());
                if (!bloomMightContain(fp)) { return std::nullopt; }
                const auto blockIndex = candidateBlock(key);
                if (!blockIndex.has_value()) { return std::nullopt; }
                const auto block = loadBlock(*blockIndex);
                if (!block) { return std::nullopt; }
                const auto rec = findInBlock(*block, key, fp, mini, false);
                if (!rec) { return std::nullopt; }
                return !rec->isTombstone();
            }

            [[nodiscard]] std::optional<bool> getInto(std::span<const uint8_t> key, std::vector<uint8_t>& out) const {
                if (!keyInRange(key)) { return std::nullopt; }
                const uint64_t fp = key.empty() ? 0 : core::computeKeyFp64(key.data(), key.size());
                const uint64_t mini = key.empty() ? 0 : core::buildMiniKey(key.data(), key.size());
                if (!bloomMightContain(fp)) { return std::nullopt; }
                const auto blockIndex = candidateBlock(key);
                if (!blockIndex.has_value()) { return std::nullopt; }
                const auto block = loadBlock(*blockIndex);
                if (!block) { return std::nullopt; }
                return findValueInBlock(*block, key, fp, mini, out);
            }

            [[nodiscard]] core::ArenaGenerator<SSTRecord> scan(std::vector<uint8_t> startKey, std::vector<uint8_t> endKey) const {
                for (size_t i = 0; i < index_.size(); ++i) {
                    const auto first = arenaKey(keyArena_, index_[i].firstKeyOffset, index_[i].firstKeyLen);
                    const auto last = arenaKey(keyArena_, index_[i].lastKeyOffset, index_[i].lastKeyLen);
                    if (!endKey.empty() && compareBytes(first, endKey) >= 0) { break; }
                    if (!startKey.empty() && compareBytes(last, startKey) < 0) { continue; }

                    const auto block = loadBlock(i);
                    if (!block) { continue; }
                    for (const uint32_t off : block->offsets) {
                        if (off + sizeof(core::SSTHdr32) > block->data.size()) { continue; }
                        const auto* hdr = reinterpret_cast<const core::SSTHdr32*>(block->data.data() + off);
                        const uint8_t* keyPtr = block->data.data() + off + sizeof(core::SSTHdr32);
                        const uint8_t* valPtr = keyPtr + hdr->kLen;
                        if (valPtr + hdr->vLen > block->data.data() + block->data.size()) { continue; }
                        std::span<const uint8_t> key{keyPtr, hdr->kLen};
                        if (!startKey.empty() && compareBytes(key, startKey) < 0) { continue; }
                        if (!endKey.empty() && compareBytes(key, endKey) >= 0) { co_return; }
                        co_yield SSTRecord{
                            .key = std::vector<uint8_t>(key.begin(), key.end()),
                            .value = std::vector<uint8_t>(valPtr, valPtr + hdr->vLen),
                            .seq = hdr->seq,
                            .flags = hdr->flags,
                            .keyFp64 = hdr->keyFp64,
                            .miniKey = hdr->miniKey
                        };
                    }
                }
            }

            [[nodiscard]] const SSTFileHeaderV2& header() const noexcept { return header_; }
            [[nodiscard]] std::span<const uint8_t> firstKey() const noexcept { return firstKey_; }
            [[nodiscard]] std::span<const uint8_t> lastKey() const noexcept { return lastKey_; }
            [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

        private:
            void readAt(uint64_t offset, void* data, size_t size) const {
                std::lock_guard lock{ioMu_};
                file_.clear();
                file_.seekg(static_cast<std::streamoff>(offset));
                file_.read(reinterpret_cast<char*>(data), static_cast<std::streamsize>(size));
                if (!file_ || file_.gcount() != static_cast<std::streamsize>(size)) { throw std::runtime_error("SSTReader: short read"); }
            }

            [[nodiscard]] std::vector<uint8_t> readVecAt(uint64_t offset, size_t size) const {
                std::vector<uint8_t> out(size);
                if (size > 0) { readAt(offset, out.data(), size); }
                return out;
            }

            [[nodiscard]] bool bloomMightContain(uint64_t fp) const noexcept {
                if (bloomHeader_.numBits == 0 || bloomHeader_.bitsSize == 0) { return true; }
                const uint8_t* bits = bloomData_.data() + sizeof(SSTBloomHeaderV2);
                const uint32_t mask = bloomHeader_.numBits - 1;
                const uint32_t h1 = static_cast<uint32_t>(fp);
                const uint32_t h2 = (static_cast<uint32_t>(fp >> 32) | 1u);
                for (uint32_t i = 0; i < bloomHeader_.numHashes; ++i) {
                    const uint32_t bit = (h1 + i * h2) & mask;
                    if ((bits[bit >> 3] & (1u << (bit & 7u))) == 0) { return false; }
                }
                return true;
            }

            [[nodiscard]] std::optional<size_t> candidateBlock(std::span<const uint8_t> key) const noexcept {
                size_t lo = 0;
                size_t hi = index_.size();
                while (lo < hi) {
                    const size_t mid = lo + (hi - lo) / 2;
                    const auto last = arenaKey(keyArena_, index_[mid].lastKeyOffset, index_[mid].lastKeyLen);
                    if (compareBytes(last, key) < 0) { lo = mid + 1; }
                    else { hi = mid; }
                }
                if (lo >= index_.size()) { return std::nullopt; }
                const auto first = arenaKey(keyArena_, index_[lo].firstKeyOffset, index_[lo].firstKeyLen);
                const auto last = arenaKey(keyArena_, index_[lo].lastKeyOffset, index_[lo].lastKeyLen);
                if (compareBytes(first, key) <= 0 && compareBytes(key, last) <= 0) { return lo; }
                return std::nullopt;
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

                if (idx >= index_.size()) { return nullptr; }
                const auto& entry = index_[idx];
                try {
                    const auto blockFile = readVecAt(entry.blockOffset, entry.blockSize);
                    if (blockFile.size() < sizeof(SSTBlockHeaderV2)) { return nullptr; }

                    SSTBlockHeaderV2 bh{};
                    std::memcpy(&bh, blockFile.data(), sizeof(bh));
                    if (bh.headerSize != sizeof(SSTBlockHeaderV2) || bh.recordCount != entry.recordCount) { return nullptr; }

                    const size_t payloadOff = sizeof(SSTBlockHeaderV2);
                    const size_t offsetsOff = payloadOff + static_cast<size_t>(bh.compressedSize);
                    const size_t crcLen = static_cast<size_t>(bh.compressedSize) + static_cast<size_t>(bh.offsetsSize);
                    if (offsetsOff > blockFile.size() || bh.offsetsSize > blockFile.size() - offsetsOff) { return nullptr; }
                    if (payloadOff > blockFile.size() || crcLen > blockFile.size() - payloadOff) { return nullptr; }

                    std::span<const uint8_t> payload{blockFile.data() + payloadOff, static_cast<size_t>(bh.compressedSize)};
                    std::span<const uint8_t> offsetsBytes{blockFile.data() + offsetsOff, static_cast<size_t>(bh.offsetsSize)};
                    if (crc32c({blockFile.data() + payloadOff, crcLen}) != bh.crc32c) { return nullptr; }

                    auto block = std::make_shared<Block>();
                    if ((bh.flags & SST_BLOCK_FLAG_COMPRESSED) != 0) {
                        block->data.resize(bh.uncompressedSize);
                        const size_t n = ZSTD_decompress(block->data.data(), block->data.size(), payload.data(), payload.size());
                        if (ZSTD_isError(n) || n != bh.uncompressedSize) { return nullptr; }
                    }
                    else { block->data.assign(payload.begin(), payload.end()); }
                    if (offsetsBytes.size() % sizeof(uint32_t) != 0) { return nullptr; }
                    block->offsets.resize(offsetsBytes.size() / sizeof(uint32_t));
                    if (!block->offsets.empty()) { std::memcpy(block->offsets.data(), offsetsBytes.data(), offsetsBytes.size()); }
                    if ((bh.flags & SST_BLOCK_FLAG_PREFIX_COMPRESSED) != 0) {
                        const auto decoded = decodePrefixCompressedBlock(block->data, block->offsets);
                        if (!decoded.has_value()) { return nullptr; }
                        block->data = std::move(decoded->data);
                        block->offsets = std::move(decoded->offsets);
                    }
                    block->bytes = block->data.size() + block->offsets.size() * sizeof(uint32_t);
                    putCache(idx, block);
                    return block;
                }
                catch (...) { return nullptr; }
            }

            void putCache(size_t idx, const std::shared_ptr<Block>& block) const {
                if (cacheCapacity_ == 0 || !block) { return; }
                std::lock_guard lock{cacheMu_};
                cacheLru_.push_front(CacheEntry{idx, block});
                cacheMap_[idx] = cacheLru_.begin();
                cacheBytes_ += block->bytes;
                while (cacheBytes_ > cacheCapacity_ && !cacheLru_.empty()) {
                    const auto& old = cacheLru_.back();
                    cacheBytes_ -= old.block ? old.block->bytes : 0;
                    cacheMap_.erase(old.blockIndex);
                    cacheLru_.pop_back();
                }
            }

            [[nodiscard]] std::optional<SSTRecord> findInBlock(
                const Block& block,
                std::span<const uint8_t> key,
                uint64_t fp,
                uint64_t mini,
                bool copyValue
            ) const {
                size_t lo = 0;
                size_t hi = block.offsets.size();
                while (lo < hi) {
                    const size_t mid = lo + (hi - lo) / 2;
                    const uint32_t off = block.offsets[mid];
                    if (off + sizeof(core::SSTHdr32) > block.data.size()) { return std::nullopt; }
                    const auto* hdr = reinterpret_cast<const core::SSTHdr32*>(block.data.data() + off);
                    const uint8_t* keyPtr = block.data.data() + off + sizeof(core::SSTHdr32);
                    const int cmp = compareRecordKey(*hdr, keyPtr, key);
                    if (cmp < 0) { lo = mid + 1; }
                    else { hi = mid; }
                }
                if (lo >= block.offsets.size()) { return std::nullopt; }
                const uint32_t off = block.offsets[lo];
                const auto* hdr = reinterpret_cast<const core::SSTHdr32*>(block.data.data() + off);
                const uint8_t* keyPtr = block.data.data() + off + sizeof(core::SSTHdr32);
                const uint8_t* valPtr = keyPtr + hdr->kLen;
                if (valPtr + hdr->vLen > block.data.data() + block.data.size()) { return std::nullopt; }
                if (!keyEquals(*hdr, keyPtr, key, fp, mini)) { return std::nullopt; }
                SSTRecord rec;
                rec.key.assign(keyPtr, keyPtr + hdr->kLen);
                if (copyValue) { rec.value.assign(valPtr, valPtr + hdr->vLen); }
                rec.seq = hdr->seq;
                rec.flags = hdr->flags;
                rec.keyFp64 = hdr->keyFp64;
                rec.miniKey = hdr->miniKey;
                return rec;
            }

            [[nodiscard]] std::optional<bool> findValueInBlock(
                const Block& block,
                std::span<const uint8_t> key,
                uint64_t fp,
                uint64_t mini,
                std::vector<uint8_t>& out
            ) const {
                size_t lo = 0;
                size_t hi = block.offsets.size();
                while (lo < hi) {
                    const size_t mid = lo + (hi - lo) / 2;
                    const uint32_t off = block.offsets[mid];
                    if (off + sizeof(core::SSTHdr32) > block.data.size()) { return std::nullopt; }
                    const auto* hdr = reinterpret_cast<const core::SSTHdr32*>(block.data.data() + off);
                    const uint8_t* keyPtr = block.data.data() + off + sizeof(core::SSTHdr32);
                    const int cmp = compareRecordKey(*hdr, keyPtr, key);
                    if (cmp < 0) { lo = mid + 1; }
                    else { hi = mid; }
                }
                if (lo >= block.offsets.size()) { return std::nullopt; }
                const uint32_t off = block.offsets[lo];
                const auto* hdr = reinterpret_cast<const core::SSTHdr32*>(block.data.data() + off);
                const uint8_t* keyPtr = block.data.data() + off + sizeof(core::SSTHdr32);
                const uint8_t* valPtr = keyPtr + hdr->kLen;
                if (valPtr + hdr->vLen > block.data.data() + block.data.size()) { return std::nullopt; }
                if (!keyEquals(*hdr, keyPtr, key, fp, mini)) { return std::nullopt; }
                if ((hdr->flags & core::SSTHdr32::FLAG_TOMBSTONE) != 0) { return false; }
                out.assign(valPtr, valPtr + hdr->vLen);
                return true;
            }

            std::filesystem::path path_;
            Options options_;
            mutable std::ifstream file_;
            mutable std::mutex ioMu_;
            SSTFileHeaderV2 header_{};
            std::vector<SSTBlockIndexEntryV2> index_;
            std::vector<uint8_t> keyArena_;
            std::vector<uint8_t> bloomData_;
            SSTBloomHeaderV2 bloomHeader_{};
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
        std::unique_ptr<SSTReader> reader(new SSTReader());
        reader->impl_ = std::make_unique<Impl>(path, options);
        if (!reader->impl_->open()) { return nullptr; }
        return reader;
    }

    SSTReader::SSTReader() = default;
    SSTReader::~SSTReader() = default;
    SSTReader::SSTReader(SSTReader&&) noexcept = default;
    SSTReader& SSTReader::operator=(SSTReader&&) noexcept = default;

    std::optional<SSTRecord> SSTReader::get(std::span<const uint8_t> key) const { return impl_->get(key); }
    std::optional<bool> SSTReader::contains(std::span<const uint8_t> key) const { return impl_->contains(key); }

    std::optional<bool> SSTReader::getInto(std::span<const uint8_t> key, std::vector<uint8_t>& out) const {
        return impl_->getInto(key, out);
    }

    core::ArenaGenerator<SSTRecord> SSTReader::scan(std::span<const uint8_t> startKey, std::span<const uint8_t> endKey) const {
        return impl_->scan(std::vector<uint8_t>(startKey.begin(), startKey.end()), std::vector<uint8_t>(endKey.begin(), endKey.end()));
    }

    bool SSTReader::keyInRange(std::span<const uint8_t> key) const noexcept { return impl_->keyInRange(key); }
    const SSTFileHeaderV2& SSTReader::header() const noexcept { return impl_->header(); }
    std::span<const uint8_t> SSTReader::firstKey() const noexcept { return impl_->firstKey(); }
    std::span<const uint8_t> SSTReader::lastKey() const noexcept { return impl_->lastKey(); }
    const std::filesystem::path& SSTReader::path() const noexcept { return impl_->path(); }
} // namespace akkaradb::engine::sst

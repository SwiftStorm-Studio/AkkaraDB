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
#include <atomic>
#include <condition_variable>
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
        // Idle descriptors are shared across all readers/managers. A read leases
        // its stream until I/O completes, so eviction never closes an active read.
        class SSTFileCache {
            struct File {
                explicit File(const std::filesystem::path& path) : stream{path, std::ios::binary} {
                    if (!stream) { throw std::runtime_error("SSTReader: cannot open " + path.string()); }
                }
                std::ifstream stream;
                std::mutex mutex;
            };
            struct Entry {
                const void* owner;
                std::shared_ptr<File> file;
            };
            struct Pin {
                size_t readers = 0;
                bool retired = false;
            };
            struct Lease {
                SSTFileCache& cache;
                std::shared_ptr<File> file;
                ~Lease() {
                    std::lock_guard lock{cache.mutex_};
                    file.reset();
                    cache.available_.notify_all();
                }
            };

        public:
            void read(const void* owner, const std::filesystem::path& path, uint64_t offset, void* data, size_t size) {
                Lease lease{*this, acquire(owner, path)};
                std::lock_guard lock{lease.file->mutex};
                auto& stream = lease.file->stream;
                stream.clear();
                stream.seekg(static_cast<std::streamoff>(offset));
                stream.read(static_cast<char*>(data), static_cast<std::streamsize>(size));
                if (!stream || stream.gcount() != static_cast<std::streamsize>(size)) {
                    throw std::runtime_error("SSTReader: short read");
                }
            }

            void pin(const std::filesystem::path& path) {
                std::lock_guard lock{mutex_};
                ++pins_[path].readers;
            }

            bool isPinned(const std::filesystem::path& path) {
                std::lock_guard lock{mutex_};
                return pins_.contains(path);
            }

            void release(const void* owner, const std::filesystem::path& path, bool retired) {
                std::lock_guard lock{mutex_};
                std::erase_if(entries_, [owner](const Entry& entry) { return entry.owner == owner; });
                const auto pin = pins_.find(path);
                if (pin != pins_.end()) {
                    pin->second.retired |= retired;
                    if (--pin->second.readers == 0) {
                        if (pin->second.retired) {
                            // Only obsolete catalog inputs are removed here.
                            // Recovery retries unlink failures after reopen.
                            std::error_code error;
                            std::filesystem::remove(path, error);
                        }
                        pins_.erase(pin);
                    }
                }
                available_.notify_all();
            }

        private:
            std::shared_ptr<File> acquire(const void* owner, const std::filesystem::path& path) {
                std::unique_lock lock{mutex_};
                for (;;) {
                    const auto existing = std::ranges::find(entries_, owner, &Entry::owner);
                    if (existing != entries_.end()) {
                        entries_.splice(entries_.begin(), entries_, existing);
                        return entries_.front().file;
                    }
                    if (entries_.size() == capacity_) {
                        const auto idle = std::ranges::find_if(entries_ | std::views::reverse,
                            [](const Entry& entry) { return entry.file.use_count() == 1; });
                        if (idle != entries_.rend()) { entries_.erase(std::prev(idle.base())); }
                    }
                    if (entries_.size() < capacity_) {
                        auto file = std::make_shared<File>(path);
                        entries_.push_front(Entry{owner, file});
                        return file;
                    }
                    available_.wait(lock);
                }
            }

            static constexpr size_t capacity_ = 64;
            std::mutex mutex_;
            std::condition_variable available_;
            std::list<Entry> entries_;
            std::unordered_map<std::filesystem::path, Pin> pins_;
        };

        std::shared_ptr<SSTFileCache> fileCache() {
            static auto cache = std::make_shared<SSTFileCache>();
            return cache;
        }

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
            std::span<const uint32_t> encodedOffsets,
            size_t decodedSize
        ) {
            DecodedBlockData out;
            out.offsets.reserve(encodedOffsets.size());

            std::vector<uint8_t> prevKey;
            size_t expectedOffset = 0;
            for (const auto off : encodedOffsets) {
                if (off != expectedOffset || off > encoded.size() || sizeof(core::SSTHdr32) > encoded.size() - off) {
                    return std::nullopt;
                }

                const auto* hdr = reinterpret_cast<const core::SSTHdr32*>(encoded.data() + off);
                const auto shared = hdr->reserved1;
                if (shared > hdr->kLen || shared > prevKey.size()) { return std::nullopt; }

                const auto suffixOffset = off + sizeof(core::SSTHdr32);
                const auto suffixLen = static_cast<size_t>(hdr->kLen) - shared;
                if (suffixLen > encoded.size() - suffixOffset || hdr->vLen > encoded.size() - suffixOffset - suffixLen) {
                    return std::nullopt;
                }
                expectedOffset = alignUpU64(suffixOffset + suffixLen + hdr->vLen, 8);
                if (expectedOffset > encoded.size()) { return std::nullopt; }
                const auto* suffixPtr = encoded.data() + suffixOffset;
                const auto* valPtr = suffixPtr + suffixLen;
                const auto recordSize = alignUpU64(sizeof(core::SSTHdr32) + hdr->kLen + hdr->vLen, 8);
                if (out.data.size() > decodedSize || recordSize > decodedSize - out.data.size()) { return std::nullopt; }

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
                out.data.resize(padded, 0);

                prevKey = std::move(fullKey);
            }

            if (expectedOffset != encoded.size()) { return std::nullopt; }
            return out;
        }
    } // namespace

    class SSTReader::BlockCache::Impl {
        public:
            struct Block {
                std::vector<uint8_t> data;
                std::vector<uint32_t> offsets;
                size_t bytes = 0;
            };

            struct Key {
                uint64_t reader;
                size_t block;
                bool operator==(const Key&) const = default;
            };
            struct Hash {
                size_t operator()(const Key& key) const noexcept {
                    const auto reader = std::hash<uint64_t>{}(key.reader);
                    return reader ^ (std::hash<size_t>{}(key.block) + size_t{0x9e3779b9} + (reader << 6) + (reader >> 2));
                }
            };
            struct Entry { Key key; std::shared_ptr<Block> block; };

            explicit Impl(uint64_t capacity) : capacity_{capacity} {}

            std::shared_ptr<Block> get(Key key) {
                std::lock_guard lock{mutex_};
                const auto found = map_.find(key);
                if (found == map_.end()) { ++misses_; return {}; }
                ++hits_;
                lru_.splice(lru_.begin(), lru_, found->second);
                return found->second->block;
            }

            std::shared_ptr<Block> put(Key key, std::shared_ptr<Block> block) {
                std::lock_guard lock{mutex_};
                // Decoding runs outside the lock. Another reader thread may
                // already have installed this same block while we were loading.
                const auto found = map_.find(key);
                if (found != map_.end()) {
                    lru_.splice(lru_.begin(), lru_, found->second);
                    return found->second->block;
                }
                if (block->bytes > capacity_ || capacity_ == 0) { return block; }
                lru_.push_front({key, block});
                try { map_.emplace(key, lru_.begin()); }
                catch (...) { lru_.pop_front(); throw; }
                // Avoid overflow even with a capacity near UINT64_MAX.
                while (bytes_ > capacity_ - block->bytes) {
                    bytes_ -= lru_.back().block->bytes;
                    map_.erase(lru_.back().key);
                    lru_.pop_back();
                }
                bytes_ += block->bytes;
                return block;
            }

            void eraseReader(uint64_t reader) {
                std::lock_guard lock{mutex_};
                for (auto it = lru_.begin(); it != lru_.end();) {
                    if (it->key.reader != reader) { ++it; continue; }
                    bytes_ -= it->block->bytes;
                    map_.erase(it->key);
                    it = lru_.erase(it);
                }
            }

            CacheStats stats() const {
                std::lock_guard lock{mutex_};
                return {capacity_, bytes_, map_.size(), hits_, misses_};
            }

        private:
            mutable std::mutex mutex_;
            std::list<Entry> lru_;
            std::unordered_map<Key, std::list<Entry>::iterator, Hash> map_;
            uint64_t capacity_, bytes_ = 0, hits_ = 0, misses_ = 0;
    };

    SSTReader::BlockCache::BlockCache(uint64_t capacityBytes) : impl_{std::make_unique<Impl>(capacityBytes)} {}
    SSTReader::BlockCache::~BlockCache() = default;
    SSTReader::CacheStats SSTReader::BlockCache::stats() const { return impl_->stats(); }

    class SSTReader::Impl {
        public:
            using Block = BlockCache::Impl::Block;

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

            Impl(std::filesystem::path path, Options options) : path_{std::move(path)},
                blockCache_{options.sharedBlockCache ? std::move(options.sharedBlockCache) :
                    std::make_shared<BlockCache>(options.blockCacheBytes)}, cacheId_{nextCacheId_.fetch_add(1)} {}

            ~Impl() {
                blockCache_->impl_->eraseReader(cacheId_);
                if (pinned_) { fileCache_->release(this, filePath_, retired_.load(std::memory_order_relaxed)); }
            }

            void retire() noexcept { retired_.store(true, std::memory_order_relaxed); }

            [[nodiscard]] bool open() {
                try {
                    // Reopened streams must not depend on a later current-directory
                    // change. The pin also protects old readers across manager reopen.
                    filePath_ = std::filesystem::canonical(path_);
                    fileCache_ = fileCache();
                    fileCache_->pin(filePath_);
                    pinned_ = true;
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
                    const auto actualFileSize = std::filesystem::file_size(filePath_);
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

                    index_.resize(header_.blockCount);
                    if (!index_.empty()) { readAt(header_.indexOffset, index_.data(), index_.size() * sizeof(SSTBlockIndexEntryV1)); }
                    keyArena_ = readVecAt(header_.keyArenaOffset, header_.keyArenaSize);
                    bloomData_ = readVecAt(header_.bloomOffset, header_.bloomSize);
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
                    return true;
                }
                catch (...) { return false; }
            }

            [[nodiscard]] bool keyInRange(std::span<const uint8_t> key) const noexcept {
                if (index_.empty()) { return false; }
                return compareBytes(firstKey_, key) <= 0 && compareBytes(key, lastKey_) <= 0;
            }

            [[nodiscard]] std::optional<SSTRecord> get(std::span<const uint8_t> key, uint64_t snapshotSeq) const {
                const auto record = getPinned(key, snapshotSeq);
                return record ? std::optional{record->toOwned()} : std::nullopt;
            }

            [[nodiscard]] std::optional<PinnedRecord> getPinned(std::span<const uint8_t> key, uint64_t snapshotSeq) const {
                const auto location = findRecord(key, snapshotSeq);
                if (!location) { return std::nullopt; }
                const auto& hdr = location->header();
                const auto* keyPtr = location->keyData();
                const auto* valPtr = keyPtr + hdr.kLen;
                return PinnedRecord{
                    .view = {keyPtr, hdr.kLen, valPtr, hdr.vLen, hdr.seq, hdr.flags, hdr.keyFp64, hdr.miniKey},
                    .lifetime = location->block
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
                auto begin = index_.begin();
                if (!startKey.empty()) {
                    begin = std::ranges::lower_bound(index_, startKey, keyLess, [this](const auto& entry) {
                        return arenaKey(keyArena_, entry.lastKeyOffset, entry.lastKeyLen);
                    });
                }
                for (auto it = begin; it != index_.end(); ++it) {
                    const auto& entry = *it;
                    const auto first = arenaKey(keyArena_, entry.firstKeyOffset, entry.firstKeyLen);
                    const auto last = arenaKey(keyArena_, entry.lastKeyOffset, entry.lastKeyLen);
                    if (!endKey.empty() && compareBytes(first, endKey) >= 0) { break; }
                    if (!startKey.empty() && compareBytes(last, startKey) < 0) { continue; }

                    const auto block = loadBlock(static_cast<size_t>(it - index_.begin()));
                    auto firstRecord = block->offsets.begin();
                    if (!startKey.empty()) {
                        firstRecord = std::ranges::lower_bound(block->offsets, startKey, keyLess, [&block](auto offset) {
                            const RecordLocation location{block, offset};
                            return std::span{location.keyData(), location.header().kLen};
                        });
                    }
                    for (const auto off : std::ranges::subrange{firstRecord, block->offsets.end()}) {
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
                if (offset > static_cast<uint64_t>(std::numeric_limits<std::streamoff>::max()) ||
                    size > static_cast<size_t>(std::numeric_limits<std::streamsize>::max())) {
                    throw std::runtime_error("SSTReader: read range is too large");
                }
                fileCache_->read(this, filePath_, offset, data, size);
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
                if (block.offsets.empty() || block.offsets.front() != 0) { throw std::runtime_error("SSTReader: corrupt first record offset"); }
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
                    const auto nextOffset = alignUpU64(payloadOffset + payloadSize, 8);
                    if (nextOffset > block.data.size() || (i + 1 < block.offsets.size() && nextOffset != block.offsets[i + 1]) || (i + 1 ==
                        block.offsets.size() && nextOffset != block.data.size())) {
                        throw std::runtime_error("SSTReader: corrupt record layout");
                    }
                }
            }

            [[nodiscard]] std::shared_ptr<Block> loadBlock(size_t idx) const {
                if (auto block = blockCache_->impl_->get({cacheId_, idx})) { return block; }

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
                // The index is checksummed; the block header is not. Never use
                // its decoded size or offset count for allocation without a bound.
                const auto prefixCompressed = (bh.flags & SST_BLOCK_FLAG_PREFIX_COMPRESSED) != 0;
                if (bh.uncompressedSize == 0 || bh.uncompressedSize > entry.uncompressedSize ||
                    (!prefixCompressed && bh.uncompressedSize != entry.uncompressedSize) ||
                    uint64_t{bh.recordCount} * sizeof(uint32_t) != bh.offsetsSize) {
                    throw std::runtime_error("SSTReader: corrupt block sizes");
                }

                constexpr auto payloadOff = sizeof(SSTBlockHeaderV1);
                const auto offsetsOff = payloadOff + bh.compressedSize;
                const auto crcLen = static_cast<size_t>(bh.compressedSize) + bh.offsetsSize;
                const auto expectedBlockSize = alignUpU64(payloadOff + crcLen, 8);
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
                    const auto frameSize = ZSTD_getFrameContentSize(payload.data(), payload.size());
                    if (frameSize == ZSTD_CONTENTSIZE_ERROR ||
                        (frameSize != ZSTD_CONTENTSIZE_UNKNOWN && frameSize != bh.uncompressedSize)) {
                        throw std::runtime_error("SSTReader: corrupt Zstd block size");
                    }
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
                if (prefixCompressed) {
                    auto decoded = decodePrefixCompressedBlock(block->data, block->offsets, entry.uncompressedSize);
                    if (!decoded.has_value()) { throw std::runtime_error("SSTReader: corrupt prefix-compressed block"); }
                    block->data = std::move(decoded->data);
                    block->offsets = std::move(decoded->offsets);
                }
                if (block->data.size() != entry.uncompressedSize) { throw std::runtime_error("SSTReader: corrupt decoded block size"); }
                validateDecodedBlock(*block, bh.recordCount);
                block->bytes = block->data.capacity() + block->offsets.capacity() * sizeof(uint32_t);
                return blockCache_->impl_->put({cacheId_, idx}, std::move(block));
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
            std::filesystem::path filePath_;
            std::shared_ptr<BlockCache> blockCache_;
            uint64_t cacheId_;
            inline static std::atomic<uint64_t> nextCacheId_{1};
            bool pinned_ = false;
            std::shared_ptr<SSTFileCache> fileCache_;
            std::atomic<bool> retired_{false};
            SSTFileHeaderV1 header_{};
            std::vector<SSTBlockIndexEntryV1> index_;
            std::vector<uint8_t> keyArena_;
            std::vector<uint8_t> bloomData_;
            SSTBloomHeaderV1 bloomHeader_{};
            std::vector<uint8_t> firstKey_;
            std::vector<uint8_t> lastKey_;

    };

    SSTRecord SSTReader::PinnedRecord::toOwned() const {
        return SSTRecord{
            .key = std::ranges::to<std::vector>(view.key()),
            .value = std::ranges::to<std::vector>(view.value()),
            .seq = view.seq(),
            .flags = view.flags(),
            .keyFp64 = view.keyFp64(),
            .miniKey = view.miniKey()
        };
    }

    std::unique_ptr<SSTReader> SSTReader::open(const std::filesystem::path& path) { return open(path, Options{}); }

    std::unique_ptr<SSTReader> SSTReader::open(const std::filesystem::path& path, const Options& options) {
        auto reader = std::unique_ptr<SSTReader>{new SSTReader()};
        reader->impl_ = std::make_unique<Impl>(path, options);
        if (!reader->impl_->open()) { return nullptr; }
        return reader;
    }

    SSTReader::SSTReader() = default;
    SSTReader::~SSTReader() = default;
    void SSTReader::retire() noexcept { impl_->retire(); }
    bool SSTReader::isPinned(const std::filesystem::path& path) {
        std::error_code error;
        const auto canonical = std::filesystem::weakly_canonical(path, error);
        // A failed path lookup cannot prove that an obsolete file is unpinned.
        return error || fileCache()->isPinned(canonical);
    }
    SSTReader::SSTReader(SSTReader&&) noexcept = default;
    SSTReader& SSTReader::operator=(SSTReader&&) noexcept = default;

    std::optional<SSTRecord> SSTReader::get(std::span<const uint8_t> key, uint64_t snapshotSeq) const {
        return impl_->get(key, snapshotSeq);
    }

    std::optional<SSTReader::PinnedRecord> SSTReader::getPinned(std::span<const uint8_t> key, uint64_t snapshotSeq) const {
        return impl_->getPinned(key, snapshotSeq);
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

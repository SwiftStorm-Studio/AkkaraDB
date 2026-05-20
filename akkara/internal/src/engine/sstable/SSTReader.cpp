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

// internal/src/engine/sstable/SSTReader.cpp
#include "engine/sstable/SSTReader.hpp"

#include <algorithm>
#include <cstring>
#include <fstream>
#include <list>
#include <mutex>
#include <stdexcept>
#include <unordered_map>

#include <zstd.h>

#include "cpu/CRC32C.hpp"
#include "core/record/KeyFingerprint.hpp"

namespace akkaradb::engine::sst {
    namespace {
        void read_at(const std::filesystem::path& path, uint64_t offset, void* data, size_t size) {
            std::ifstream in(path, std::ios::binary);
            if (!in) { throw std::runtime_error("SSTReader: cannot open " + path.string()); }
            in.seekg(static_cast<std::streamoff>(offset));
            in.read(reinterpret_cast<char*>(data), static_cast<std::streamsize>(size));
            if (!in || in.gcount() != static_cast<std::streamsize>(size)) { throw std::runtime_error("SSTReader: short read"); }
        }

        [[nodiscard]] std::vector<uint8_t> read_vec_at(const std::filesystem::path& path, uint64_t offset, size_t size) {
            std::vector<uint8_t> out(size);
            if (size > 0) { read_at(path, offset, out.data(), size); }
            return out;
        }

        [[nodiscard]] uint32_t crc32c(std::span<const uint8_t> bytes) noexcept {
            return cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size());
        }

        [[nodiscard]] int compare_bytes(std::span<const uint8_t> a, std::span<const uint8_t> b) noexcept {
            const size_t n = std::min(a.size(), b.size());
            if (n > 0) {
                const int c = std::memcmp(a.data(), b.data(), n);
                if (c != 0) { return c < 0 ? -1 : 1; }
            }
            if (a.size() < b.size()) { return -1; }
            if (a.size() > b.size()) { return 1; }
            return 0;
        }

        [[nodiscard]] bool key_equals(
            const core::SSTHdr32& hdr,
            const uint8_t* key_data,
            std::span<const uint8_t> target,
            uint64_t target_fp,
            uint64_t target_mini
        ) noexcept {
            if (hdr.k_len != target.size()) { return false; }
            if (hdr.key_fp64 != target_fp || hdr.mini_key != target_mini) { return false; }
            if (hdr.k_len == 0) { return true; }
            return std::memcmp(key_data, target.data(), hdr.k_len) == 0;
        }

        [[nodiscard]] int compare_record_key(const core::SSTHdr32& hdr, const uint8_t* key_data, std::span<const uint8_t> target) noexcept {
            return compare_bytes({key_data, hdr.k_len}, target);
        }

        [[nodiscard]] std::span<const uint8_t> arena_key(const std::vector<uint8_t>& arena, uint32_t off, uint32_t len) noexcept {
            if (off > arena.size() || len > arena.size() - off) { return {}; }
            return {arena.data() + off, len};
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
                size_t block_index;
                std::shared_ptr<Block> block;
            };

            Impl(std::filesystem::path path, Options options) : path_{std::move(path)}, options_{options} {}

            [[nodiscard]] bool open() {
                try {
                    file_.open(path_, std::ios::binary);
                    if (!file_) { return false; }

                    read_at(0, &header_, sizeof(header_));
                    if (header_.magic != SST_MAGIC_V2 || header_.version != SST_VERSION_V2 || header_.header_size != sizeof(SSTFileHeaderV2)) { return false; }
                    const uint32_t stored = header_.crc32c;
                    header_.crc32c = 0;
                    const uint32_t computed = cpu::CRC32C(reinterpret_cast<const std::byte*>(&header_), sizeof(header_));
                    header_.crc32c = stored;
                    if (stored != computed) { return false; }
                    if (!std::filesystem::exists(path_) || std::filesystem::file_size(path_) < header_.file_size) { return false; }

                    SSTFooterV2 footer{};
                    read_at(header_.footer_offset, &footer, sizeof(footer));
                    const uint32_t stored_footer = footer.footer_crc32c;
                    footer.footer_crc32c = 0;
                    const uint32_t computed_footer = cpu::CRC32C(reinterpret_cast<const std::byte*>(&footer), sizeof(footer));
                    if (stored_footer != computed_footer || footer.magic != SST_FOOTER_MAGIC_V2 || footer.version != SST_VERSION_V2 || footer.header_crc32c !=
                        stored) { return false; }

                    index_.resize(static_cast<size_t>(header_.block_count));
                    if (!index_.empty()) { read_at(header_.index_offset, index_.data(), index_.size() * sizeof(SSTBlockIndexEntryV2)); }
                    key_arena_ = read_vec_at(header_.key_arena_offset, static_cast<size_t>(header_.key_arena_size));
                    bloom_data_ = read_vec_at(header_.bloom_offset, static_cast<size_t>(header_.bloom_size));
                    if (bloom_data_.size() < sizeof(SSTBloomHeaderV2)) { return false; }
                    std::memcpy(&bloom_header_, bloom_data_.data(), sizeof(bloom_header_));
                    if (bloom_header_.bits_size + sizeof(SSTBloomHeaderV2) != bloom_data_.size()) { return false; }
                    if (!index_.empty()) {
                        first_key_.assign(
                            arena_key(key_arena_, index_.front().first_key_offset, index_.front().first_key_len).begin(),
                            arena_key(key_arena_, index_.front().first_key_offset, index_.front().first_key_len).end()
                        );
                        last_key_.assign(
                            arena_key(key_arena_, index_.back().last_key_offset, index_.back().last_key_len).begin(),
                            arena_key(key_arena_, index_.back().last_key_offset, index_.back().last_key_len).end()
                        );
                    }
                    cache_capacity_ = options_.block_cache_bytes;
                    return true;
                }
                catch (...) { return false; }
            }

            [[nodiscard]] bool key_in_range(std::span<const uint8_t> key) const noexcept {
                if (index_.empty()) { return false; }
                return compare_bytes(first_key_, key) <= 0 && compare_bytes(key, last_key_) <= 0;
            }

            [[nodiscard]] std::optional<SSTRecord> get(std::span<const uint8_t> key) const {
                if (!key_in_range(key)) { return std::nullopt; }
                const uint64_t fp = key.empty() ? 0 : core::compute_key_fp64(key.data(), key.size());
                const uint64_t mini = key.empty() ? 0 : core::build_mini_key(key.data(), key.size());
                if (!bloom_might_contain(fp)) { return std::nullopt; }
                const auto block_index = candidate_block(key);
                if (!block_index.has_value()) { return std::nullopt; }
                const auto block = load_block(*block_index);
                if (!block) { return std::nullopt; }
                return find_in_block(*block, key, fp, mini, true);
            }

            [[nodiscard]] std::optional<bool> contains(std::span<const uint8_t> key) const {
                if (!key_in_range(key)) { return std::nullopt; }
                const uint64_t fp = key.empty() ? 0 : core::compute_key_fp64(key.data(), key.size());
                const uint64_t mini = key.empty() ? 0 : core::build_mini_key(key.data(), key.size());
                if (!bloom_might_contain(fp)) { return std::nullopt; }
                const auto block_index = candidate_block(key);
                if (!block_index.has_value()) { return std::nullopt; }
                const auto block = load_block(*block_index);
                if (!block) { return std::nullopt; }
                const auto rec = find_in_block(*block, key, fp, mini, false);
                if (!rec) { return std::nullopt; }
                return !rec->is_tombstone();
            }

            [[nodiscard]] std::optional<bool> get_into(std::span<const uint8_t> key, std::vector<uint8_t>& out) const {
                if (!key_in_range(key)) { return std::nullopt; }
                const uint64_t fp = key.empty() ? 0 : core::compute_key_fp64(key.data(), key.size());
                const uint64_t mini = key.empty() ? 0 : core::build_mini_key(key.data(), key.size());
                if (!bloom_might_contain(fp)) { return std::nullopt; }
                const auto block_index = candidate_block(key);
                if (!block_index.has_value()) { return std::nullopt; }
                const auto block = load_block(*block_index);
                if (!block) { return std::nullopt; }
                return find_value_in_block(*block, key, fp, mini, out);
            }

            [[nodiscard]] core::ArenaGenerator<SSTRecord> scan(std::vector<uint8_t> start_key, std::vector<uint8_t> end_key) const {
                for (size_t i = 0; i < index_.size(); ++i) {
                    const auto first = arena_key(key_arena_, index_[i].first_key_offset, index_[i].first_key_len);
                    const auto last = arena_key(key_arena_, index_[i].last_key_offset, index_[i].last_key_len);
                    if (!end_key.empty() && compare_bytes(first, end_key) >= 0) { break; }
                    if (!start_key.empty() && compare_bytes(last, start_key) < 0) { continue; }

                    const auto block = load_block(i);
                    if (!block) { continue; }
                    for (const uint32_t off : block->offsets) {
                        if (off + sizeof(core::SSTHdr32) > block->data.size()) { continue; }
                        const auto* hdr = reinterpret_cast<const core::SSTHdr32*>(block->data.data() + off);
                        const uint8_t* key_ptr = block->data.data() + off + sizeof(core::SSTHdr32);
                        const uint8_t* val_ptr = key_ptr + hdr->k_len;
                        if (val_ptr + hdr->v_len > block->data.data() + block->data.size()) { continue; }
                        std::span<const uint8_t> key{key_ptr, hdr->k_len};
                        if (!start_key.empty() && compare_bytes(key, start_key) < 0) { continue; }
                        if (!end_key.empty() && compare_bytes(key, end_key) >= 0) { co_return; }
                        co_yield SSTRecord{
                            .key = std::vector<uint8_t>(key.begin(), key.end()),
                            .value = std::vector<uint8_t>(val_ptr, val_ptr + hdr->v_len),
                            .seq = hdr->seq,
                            .flags = hdr->flags,
                            .key_fp64 = hdr->key_fp64,
                            .mini_key = hdr->mini_key
                        };
                    }
                }
            }

            [[nodiscard]] const SSTFileHeaderV2& header() const noexcept { return header_; }
            [[nodiscard]] std::span<const uint8_t> first_key() const noexcept { return first_key_; }
            [[nodiscard]] std::span<const uint8_t> last_key() const noexcept { return last_key_; }
            [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

        private:
            void read_at(uint64_t offset, void* data, size_t size) const {
                std::lock_guard lock{io_mu_};
                file_.clear();
                file_.seekg(static_cast<std::streamoff>(offset));
                file_.read(reinterpret_cast<char*>(data), static_cast<std::streamsize>(size));
                if (!file_ || file_.gcount() != static_cast<std::streamsize>(size)) { throw std::runtime_error("SSTReader: short read"); }
            }

            [[nodiscard]] std::vector<uint8_t> read_vec_at(uint64_t offset, size_t size) const {
                std::vector<uint8_t> out(size);
                if (size > 0) { read_at(offset, out.data(), size); }
                return out;
            }

            [[nodiscard]] bool bloom_might_contain(uint64_t fp) const noexcept {
                if (bloom_header_.num_bits == 0 || bloom_header_.bits_size == 0) { return true; }
                const uint8_t* bits = bloom_data_.data() + sizeof(SSTBloomHeaderV2);
                const uint32_t mask = bloom_header_.num_bits - 1;
                const uint32_t h1 = static_cast<uint32_t>(fp);
                const uint32_t h2 = (static_cast<uint32_t>(fp >> 32) | 1u);
                for (uint32_t i = 0; i < bloom_header_.num_hashes; ++i) {
                    const uint32_t bit = (h1 + i * h2) & mask;
                    if ((bits[bit >> 3] & (1u << (bit & 7u))) == 0) { return false; }
                }
                return true;
            }

            [[nodiscard]] std::optional<size_t> candidate_block(std::span<const uint8_t> key) const noexcept {
                size_t lo = 0;
                size_t hi = index_.size();
                while (lo < hi) {
                    const size_t mid = lo + (hi - lo) / 2;
                    const auto last = arena_key(key_arena_, index_[mid].last_key_offset, index_[mid].last_key_len);
                    if (compare_bytes(last, key) < 0) { lo = mid + 1; }
                    else { hi = mid; }
                }
                if (lo >= index_.size()) { return std::nullopt; }
                const auto first = arena_key(key_arena_, index_[lo].first_key_offset, index_[lo].first_key_len);
                const auto last = arena_key(key_arena_, index_[lo].last_key_offset, index_[lo].last_key_len);
                if (compare_bytes(first, key) <= 0 && compare_bytes(key, last) <= 0) { return lo; }
                return std::nullopt;
            }

            [[nodiscard]] std::shared_ptr<Block> load_block(size_t idx) const {
                {
                    std::lock_guard lock{cache_mu_};
                    auto it = cache_map_.find(idx);
                    if (it != cache_map_.end()) {
                        cache_lru_.splice(cache_lru_.begin(), cache_lru_, it->second);
                        return it->second->block;
                    }
                }

                if (idx >= index_.size()) { return nullptr; }
                const auto& entry = index_[idx];
                try {
                    const auto block_file = read_vec_at(entry.block_offset, entry.block_size);
                    if (block_file.size() < sizeof(SSTBlockHeaderV2)) { return nullptr; }

                    SSTBlockHeaderV2 bh{};
                    std::memcpy(&bh, block_file.data(), sizeof(bh));
                    if (bh.header_size != sizeof(SSTBlockHeaderV2) || bh.record_count != entry.record_count) { return nullptr; }

                    const size_t payload_off = sizeof(SSTBlockHeaderV2);
                    const size_t offsets_off = payload_off + static_cast<size_t>(bh.compressed_size);
                    const size_t crc_len = static_cast<size_t>(bh.compressed_size) + static_cast<size_t>(bh.offsets_size);
                    if (offsets_off > block_file.size() || bh.offsets_size > block_file.size() - offsets_off) { return nullptr; }
                    if (payload_off > block_file.size() || crc_len > block_file.size() - payload_off) { return nullptr; }

                    std::span<const uint8_t> payload{block_file.data() + payload_off, static_cast<size_t>(bh.compressed_size)};
                    std::span<const uint8_t> offsets_bytes{block_file.data() + offsets_off, static_cast<size_t>(bh.offsets_size)};
                    if (crc32c({block_file.data() + payload_off, crc_len}) != bh.crc32c) { return nullptr; }

                    auto block = std::make_shared<Block>();
                    if ((bh.flags & SST_BLOCK_FLAG_COMPRESSED) != 0) {
                        block->data.resize(bh.uncompressed_size);
                        const size_t n = ZSTD_decompress(block->data.data(), block->data.size(), payload.data(), payload.size());
                        if (ZSTD_isError(n) || n != bh.uncompressed_size) { return nullptr; }
                    }
                    else { block->data.assign(payload.begin(), payload.end()); }
                    if (offsets_bytes.size() % sizeof(uint32_t) != 0) { return nullptr; }
                    block->offsets.resize(offsets_bytes.size() / sizeof(uint32_t));
                    if (!block->offsets.empty()) { std::memcpy(block->offsets.data(), offsets_bytes.data(), offsets_bytes.size()); }
                    block->bytes = block->data.size() + offsets_bytes.size();
                    put_cache(idx, block);
                    return block;
                }
                catch (...) { return nullptr; }
            }

            void put_cache(size_t idx, const std::shared_ptr<Block>& block) const {
                if (cache_capacity_ == 0 || !block) { return; }
                std::lock_guard lock{cache_mu_};
                cache_lru_.push_front(CacheEntry{idx, block});
                cache_map_[idx] = cache_lru_.begin();
                cache_bytes_ += block->bytes;
                while (cache_bytes_ > cache_capacity_ && !cache_lru_.empty()) {
                    const auto& old = cache_lru_.back();
                    cache_bytes_ -= old.block ? old.block->bytes : 0;
                    cache_map_.erase(old.block_index);
                    cache_lru_.pop_back();
                }
            }

            [[nodiscard]] std::optional<SSTRecord> find_in_block(
                const Block& block,
                std::span<const uint8_t> key,
                uint64_t fp,
                uint64_t mini,
                bool copy_value
            ) const {
                size_t lo = 0;
                size_t hi = block.offsets.size();
                while (lo < hi) {
                    const size_t mid = lo + (hi - lo) / 2;
                    const uint32_t off = block.offsets[mid];
                    if (off + sizeof(core::SSTHdr32) > block.data.size()) { return std::nullopt; }
                    const auto* hdr = reinterpret_cast<const core::SSTHdr32*>(block.data.data() + off);
                    const uint8_t* key_ptr = block.data.data() + off + sizeof(core::SSTHdr32);
                    const int cmp = compare_record_key(*hdr, key_ptr, key);
                    if (cmp < 0) { lo = mid + 1; }
                    else { hi = mid; }
                }
                if (lo >= block.offsets.size()) { return std::nullopt; }
                const uint32_t off = block.offsets[lo];
                const auto* hdr = reinterpret_cast<const core::SSTHdr32*>(block.data.data() + off);
                const uint8_t* key_ptr = block.data.data() + off + sizeof(core::SSTHdr32);
                const uint8_t* val_ptr = key_ptr + hdr->k_len;
                if (val_ptr + hdr->v_len > block.data.data() + block.data.size()) { return std::nullopt; }
                if (!key_equals(*hdr, key_ptr, key, fp, mini)) { return std::nullopt; }
                SSTRecord rec;
                rec.key.assign(key_ptr, key_ptr + hdr->k_len);
                if (copy_value) { rec.value.assign(val_ptr, val_ptr + hdr->v_len); }
                rec.seq = hdr->seq;
                rec.flags = hdr->flags;
                rec.key_fp64 = hdr->key_fp64;
                rec.mini_key = hdr->mini_key;
                return rec;
            }

            [[nodiscard]] std::optional<bool> find_value_in_block(
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
                    const uint8_t* key_ptr = block.data.data() + off + sizeof(core::SSTHdr32);
                    const int cmp = compare_record_key(*hdr, key_ptr, key);
                    if (cmp < 0) { lo = mid + 1; }
                    else { hi = mid; }
                }
                if (lo >= block.offsets.size()) { return std::nullopt; }
                const uint32_t off = block.offsets[lo];
                const auto* hdr = reinterpret_cast<const core::SSTHdr32*>(block.data.data() + off);
                const uint8_t* key_ptr = block.data.data() + off + sizeof(core::SSTHdr32);
                const uint8_t* val_ptr = key_ptr + hdr->k_len;
                if (val_ptr + hdr->v_len > block.data.data() + block.data.size()) { return std::nullopt; }
                if (!key_equals(*hdr, key_ptr, key, fp, mini)) { return std::nullopt; }
                if ((hdr->flags & core::SSTHdr32::FLAG_TOMBSTONE) != 0) { return false; }
                out.assign(val_ptr, val_ptr + hdr->v_len);
                return true;
            }

            std::filesystem::path path_;
            Options options_;
            mutable std::ifstream file_;
            mutable std::mutex io_mu_;
            SSTFileHeaderV2 header_{};
            std::vector<SSTBlockIndexEntryV2> index_;
            std::vector<uint8_t> key_arena_;
            std::vector<uint8_t> bloom_data_;
            SSTBloomHeaderV2 bloom_header_{};
            std::vector<uint8_t> first_key_;
            std::vector<uint8_t> last_key_;

            mutable std::mutex cache_mu_;
            mutable std::list<CacheEntry> cache_lru_;
            mutable std::unordered_map<size_t, std::list<CacheEntry>::iterator> cache_map_;
            mutable uint64_t cache_bytes_{0};
            uint64_t cache_capacity_{0};
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
    std::optional<bool> SSTReader::get_into(std::span<const uint8_t> key, std::vector<uint8_t>& out) const { return impl_->get_into(key, out); }

    core::ArenaGenerator<SSTRecord> SSTReader::scan(std::span<const uint8_t> start_key, std::span<const uint8_t> end_key) const {
        return impl_->scan(std::vector<uint8_t>(start_key.begin(), start_key.end()), std::vector<uint8_t>(end_key.begin(), end_key.end()));
    }

    bool SSTReader::key_in_range(std::span<const uint8_t> key) const noexcept { return impl_->key_in_range(key); }
    const SSTFileHeaderV2& SSTReader::header() const noexcept { return impl_->header(); }
    std::span<const uint8_t> SSTReader::first_key() const noexcept { return impl_->first_key(); }
    std::span<const uint8_t> SSTReader::last_key() const noexcept { return impl_->last_key(); }
    const std::filesystem::path& SSTReader::path() const noexcept { return impl_->path(); }
} // namespace akkaradb::engine::sst

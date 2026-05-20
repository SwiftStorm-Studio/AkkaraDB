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

// internal/src/engine/sstable/SSTWriter.cpp
#include "engine/sstable/SSTWriter.hpp"

#include <algorithm>
#include <bit>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <string>

#include <zstd.h>

#include "core/record/SSTHdr32.hpp"
#include "cpu/CRC32C.hpp"

namespace akkaradb::engine::sst {
    namespace {
        [[nodiscard]] uint32_t crc32c(std::span<const uint8_t> bytes) noexcept {
            return cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size());
        }

        template <typename T>
        void append_pod(std::vector<uint8_t>& out, const T& value) {
            const auto* p = reinterpret_cast<const uint8_t*>(&value);
            out.insert(out.end(), p, p + sizeof(T));
        }

        void write_exact(std::ofstream& out, const void* data, size_t size) {
            out.write(reinterpret_cast<const char*>(data), static_cast<std::streamsize>(size));
            if (!out) { throw std::runtime_error("SSTWriter: write failed"); }
        }

        [[nodiscard]] uint32_t next_pow2_u32(uint64_t value) noexcept {
            if (value <= 2) { return 2; }
            if (value > (1ULL << 31)) { return 1u << 31; }
            return static_cast<uint32_t>(std::bit_ceil(value));
        }

        struct BloomBuild {
            SSTBloomHeaderV2 header{};
            std::vector<uint8_t> bits;

            explicit BloomBuild(size_t entries, uint32_t bits_per_key) {
                const uint64_t requested_bits = std::max<uint64_t>(512, static_cast<uint64_t>(entries) * std::max<uint32_t>(1, bits_per_key));
                header.num_bits = next_pow2_u32(requested_bits);
                header.num_hashes = std::max<uint32_t>(1, static_cast<uint32_t>(static_cast<double>(bits_per_key) * 0.69));
                header.bits_size = header.num_bits / 8;
                bits.assign(header.bits_size, 0);
            }

            void add(uint64_t fp64) {
                const uint32_t mask = header.num_bits - 1;
                const uint32_t h1 = static_cast<uint32_t>(fp64);
                const uint32_t h2 = (static_cast<uint32_t>(fp64 >> 32) | 1u);
                for (uint32_t i = 0; i < header.num_hashes; ++i) {
                    const uint32_t bit = (h1 + i * h2) & mask;
                    bits[bit >> 3] = static_cast<uint8_t>(bits[bit >> 3] | (1u << (bit & 7u)));
                }
            }
        };

        struct PendingBlock {
            std::vector<uint8_t> raw;
            std::vector<uint32_t> offsets;
            std::vector<uint8_t> first_key;
            std::vector<uint8_t> last_key;
            uint64_t first_seq = 0;
            uint64_t last_seq = 0;
            uint64_t first_fp = 0;
            uint64_t last_fp = 0;
            uint64_t first_mini = 0;
            uint64_t last_mini = 0;

            [[nodiscard]] bool empty() const noexcept { return offsets.empty(); }
        };

        [[nodiscard]] uint32_t add_key(std::vector<uint8_t>& arena, std::span<const uint8_t> key) {
            if (arena.size() > UINT32_MAX) { throw std::runtime_error("SSTWriter: key arena too large"); }
            const uint32_t off = static_cast<uint32_t>(arena.size());
            arena.insert(arena.end(), key.begin(), key.end());
            return off;
        }

        void append_record(PendingBlock& block, const core::RecordView& rec) {
            if (block.raw.size() > UINT32_MAX) { throw std::runtime_error("SSTWriter: block too large"); }
            block.offsets.push_back(static_cast<uint32_t>(block.raw.size()));

            const auto key = rec.key();
            const auto value = rec.value();
            core::SSTHdr32 hdr{
                .seq = rec.seq(),
                .k_len = static_cast<uint16_t>(key.size()),
                .v_len = static_cast<uint16_t>(value.size()),
                .flags = rec.flags(),
                .reserved0 = 0,
                .reserved1 = 0,
                .key_fp64 = rec.key_fp64(),
                .mini_key = rec.mini_key()
            };

            if (key.size() > UINT16_MAX || value.size() > UINT16_MAX) { throw std::invalid_argument("SSTWriter: key/value length exceeds u16"); }

            append_pod(block.raw, hdr);
            block.raw.insert(block.raw.end(), key.begin(), key.end());
            block.raw.insert(block.raw.end(), value.begin(), value.end());
            const uint64_t padded = align_up_u64(static_cast<uint64_t>(block.raw.size()), 8);
            block.raw.resize(static_cast<size_t>(padded), 0);

            if (block.offsets.size() == 1) {
                block.first_key.assign(key.begin(), key.end());
                block.first_seq = hdr.seq;
                block.first_fp = hdr.key_fp64;
                block.first_mini = hdr.mini_key;
            }
            block.last_key.assign(key.begin(), key.end());
            block.last_seq = hdr.seq;
            block.last_fp = hdr.key_fp64;
            block.last_mini = hdr.mini_key;
        }

        [[nodiscard]] std::vector<uint8_t> compress_or_raw(const std::vector<uint8_t>& raw, SSTWriter::Codec codec, uint32_t& flags) {
            flags = SST_BLOCK_FLAG_RAW;
            if (codec != SSTWriter::Codec::Zstd || raw.empty()) { return raw; }

            const size_t bound = ZSTD_compressBound(raw.size());
            std::vector<uint8_t> compressed(bound);
            const size_t n = ZSTD_compress(compressed.data(), compressed.size(), raw.data(), raw.size(), 1);
            if (ZSTD_isError(n) || n >= raw.size()) { return raw; }
            compressed.resize(n);
            flags = SST_BLOCK_FLAG_COMPRESSED;
            return compressed;
        }
    } // namespace

    SSTWriter::Result SSTWriter::write(const std::filesystem::path& path, std::span<const core::RecordView> records) {
        return write(path, records, Options{});
    }

    SSTWriter::Result SSTWriter::write(const std::filesystem::path& path, std::span<const core::RecordView> records, const Options& options) {
        if (records.empty()) { throw std::invalid_argument("SSTWriter::write: records must be non-empty"); }
        if (options.block_size < 4096 || (options.block_size & 7u) != 0) {
            throw std::invalid_argument("SSTWriter::write: block_size must be >=4096 and 8-byte aligned");
        }

        for (size_t i = 1; i < records.size(); ++i) {
            if (records[i - 1].compare_key(records[i]) > 0) { throw std::invalid_argument("SSTWriter::write: records must be sorted by key"); }
        }

        if (path.has_parent_path()) { std::filesystem::create_directories(path.parent_path()); }
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        if (!out) { throw std::runtime_error("SSTWriter: cannot open " + path.string()); }

        SSTFileHeaderV2 header{};
        write_exact(out, &header, sizeof(header));

        Result result;
        result.path = path;
        result.entry_count = records.size();
        result.first_key.assign(records.front().key().begin(), records.front().key().end());
        result.last_key.assign(records.back().key().begin(), records.back().key().end());

        BloomBuild bloom(records.size(), options.bloom_bits_per_key);
        for (const auto& rec : records) {
            bloom.add(rec.key_fp64());
            result.min_seq = std::min(result.min_seq, rec.seq());
            result.max_seq = std::max(result.max_seq, rec.seq());
        }

        std::vector<SSTBlockIndexEntryV2> index;
        std::vector<uint8_t> key_arena;
        PendingBlock block;

        auto flush_block = [&]() {
            if (block.empty()) { return; }

            uint32_t block_flags = 0;
            std::vector<uint8_t> payload = compress_or_raw(block.raw, options.codec, block_flags);
            std::vector<uint8_t> offsets_bytes;
            offsets_bytes.reserve(block.offsets.size() * sizeof(uint32_t));
            for (const uint32_t off : block.offsets) { append_pod(offsets_bytes, off); }

            const uint64_t block_offset = static_cast<uint64_t>(out.tellp());
            SSTBlockHeaderV2 bh{};
            bh.header_size = sizeof(SSTBlockHeaderV2);
            bh.flags = block_flags;
            bh.record_count = static_cast<uint32_t>(block.offsets.size());
            bh.compressed_size = static_cast<uint32_t>(payload.size());
            bh.uncompressed_size = static_cast<uint32_t>(block.raw.size());
            bh.offsets_size = static_cast<uint32_t>(offsets_bytes.size());
            bh.first_seq = block.first_seq;
            bh.last_seq = block.last_seq;
            bh.first_key_fp64 = block.first_fp;
            bh.last_key_fp64 = block.last_fp;

            std::vector<uint8_t> crc_input;
            crc_input.reserve(payload.size() + offsets_bytes.size());
            crc_input.insert(crc_input.end(), payload.begin(), payload.end());
            crc_input.insert(crc_input.end(), offsets_bytes.begin(), offsets_bytes.end());
            bh.crc32c = crc32c(crc_input);

            write_exact(out, &bh, sizeof(bh));
            write_exact(out, payload.data(), payload.size());
            write_exact(out, offsets_bytes.data(), offsets_bytes.size());

            const uint64_t next = align_up_u64(static_cast<uint64_t>(out.tellp()), 8);
            const size_t pad = static_cast<size_t>(next - static_cast<uint64_t>(out.tellp()));
            if (pad > 0) {
                const uint8_t zeros[8]{};
                write_exact(out, zeros, pad);
            }

            const uint32_t first_key_off = add_key(key_arena, block.first_key);
            const uint32_t last_key_off = add_key(key_arena, block.last_key);
            index.push_back(
                SSTBlockIndexEntryV2{
                    .block_offset = block_offset,
                    .block_size = static_cast<uint32_t>(next - block_offset),
                    .uncompressed_size = static_cast<uint32_t>(block.raw.size()),
                    .first_mini_key = block.first_mini,
                    .last_mini_key = block.last_mini,
                    .first_key_fp64 = block.first_fp,
                    .last_key_fp64 = block.last_fp,
                    .first_key_offset = first_key_off,
                    .first_key_len = static_cast<uint32_t>(block.first_key.size()),
                    .last_key_offset = last_key_off,
                    .last_key_len = static_cast<uint32_t>(block.last_key.size()),
                    .record_count = static_cast<uint32_t>(block.offsets.size()),
                    .flags = block_flags
                }
            );

            block = PendingBlock{};
        };

        for (const auto& rec : records) {
            const uint64_t estimated = align_up_u64(32 + rec.key_size() + rec.value_size(), 8);
            if (!block.empty() && block.raw.size() + estimated > options.block_size) { flush_block(); }
            append_record(block, rec);
        }
        flush_block();

        const uint64_t index_offset = static_cast<uint64_t>(out.tellp());
        for (const auto& entry : index) { write_exact(out, &entry, sizeof(entry)); }

        const uint64_t key_arena_offset = static_cast<uint64_t>(out.tellp());
        if (!key_arena.empty()) { write_exact(out, key_arena.data(), key_arena.size()); }

        const uint64_t bloom_offset = static_cast<uint64_t>(out.tellp());
        write_exact(out, &bloom.header, sizeof(bloom.header));
        write_exact(out, bloom.bits.data(), bloom.bits.size());

        const uint64_t footer_offset = static_cast<uint64_t>(out.tellp());
        SSTFooterV2 footer{};
        footer.file_size = footer_offset + sizeof(SSTFooterV2);
        footer.index_offset = index_offset;
        footer.key_arena_offset = key_arena_offset;
        footer.bloom_offset = bloom_offset;
        footer.magic = SST_FOOTER_MAGIC_V2;
        footer.version = SST_VERSION_V2;

        header.magic = SST_MAGIC_V2;
        header.version = SST_VERSION_V2;
        header.header_size = sizeof(SSTFileHeaderV2);
        header.flags = options.codec == Codec::Zstd ? SST_FILE_FLAG_BLOCK_ZSTD : 0;
        header.level = static_cast<uint32_t>(options.level);
        header.file_size = footer.file_size;
        header.entry_count = records.size();
        header.block_count = index.size();
        header.data_offset = sizeof(SSTFileHeaderV2);
        header.index_offset = index_offset;
        header.index_size = index.size() * sizeof(SSTBlockIndexEntryV2);
        header.key_arena_offset = key_arena_offset;
        header.key_arena_size = key_arena.size();
        header.bloom_offset = bloom_offset;
        header.bloom_size = sizeof(SSTBloomHeaderV2) + bloom.bits.size();
        header.footer_offset = footer_offset;
        header.min_seq = result.min_seq;
        header.max_seq = result.max_seq;
        header.block_size = options.block_size;
        header.crc32c = 0;
        header.crc32c = cpu::CRC32C(reinterpret_cast<const std::byte*>(&header), sizeof(header));

        footer.header_crc32c = header.crc32c;
        footer.footer_crc32c = 0;
        footer.footer_crc32c = cpu::CRC32C(reinterpret_cast<const std::byte*>(&footer), sizeof(footer));
        write_exact(out, &footer, sizeof(footer));

        out.seekp(0);
        write_exact(out, &header, sizeof(header));
        out.close();
        if (!out) { throw std::runtime_error("SSTWriter: close failed"); }

        result.file_size_bytes = header.file_size;
        return result;
    }
} // namespace akkaradb::engine::sst

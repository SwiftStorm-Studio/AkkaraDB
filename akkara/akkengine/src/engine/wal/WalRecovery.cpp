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

// akkengine/src/engine/wal/WalRecovery.cpp
#include "akk/engine/wal/WalRecovery.hpp"

#include "akk/engine/memtable/MemTable.hpp"
#include "akk/engine/wal/WalFraming.hpp"

#include <algorithm>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace akkaradb::engine::wal {
    namespace fs = std::filesystem;

    namespace {
        struct SegmentFile {
            fs::path path;
            WalSegmentHeader header;
        };

        [[nodiscard]] bool read_exact(std::ifstream& file, uint8_t* out, size_t len) {
            if (len == 0) { return true; }
            file.read(reinterpret_cast<char*>(out), static_cast<std::streamsize>(len));
            return file.good() || file.gcount() == static_cast<std::streamsize>(len);
        }

        [[nodiscard]] std::vector<SegmentFile> list_segments(const fs::path& wal_dir, WalRecoveryResult& result) {
            std::vector<SegmentFile> files;
            if (!fs::exists(wal_dir)) { return files; }
            if (!fs::is_directory(wal_dir)) { throw std::runtime_error("WAL recovery path is not a directory: " + wal_dir.string()); }

            for (const auto& entry : fs::directory_iterator(wal_dir)) {
                if (!entry.is_regular_file() || entry.path().extension() != ".akwal") { continue; }
                ++result.segments_seen;

                std::ifstream file(entry.path(), std::ios::binary);
                if (!file) { throw std::runtime_error("WAL recovery failed to open segment: " + entry.path().string()); }

                uint8_t hdr_buf[WalSegmentHeader::SIZE]{};
                if (!read_exact(file, hdr_buf, WalSegmentHeader::SIZE)) {
                    ++result.corrupt_segments;
                    continue;
                }

                const WalSegmentHeader hdr = WalSegmentHeader::deserialize(hdr_buf);
                if (!hdr.verify_magic() || !hdr.verify_version() || !hdr.verify_checksum()) {
                    ++result.corrupt_segments;
                    continue;
                }

                files.push_back(SegmentFile{entry.path(), hdr});
            }

            std::sort(
                files.begin(),
                files.end(),
                [](const SegmentFile& a, const SegmentFile& b) {
                    if (a.header.shard_id != b.header.shard_id) { return a.header.shard_id < b.header.shard_id; }
                    return a.header.segment_id < b.header.segment_id;
                }
            );
            return files;
        }

        void recover_segment(
            const SegmentFile& segment,
            const WalRecoveryOptions& options,
            const WalRecovery::Callback& callback,
            WalRecoveryResult& result
        ) {
            std::ifstream file(segment.path, std::ios::binary);
            if (!file) { throw std::runtime_error("WAL recovery failed to open segment: " + segment.path.string()); }
            file.seekg(WalSegmentHeader::SIZE, std::ios::beg);

            bool replayed_any = false;
            std::vector<uint8_t> key;
            std::vector<uint8_t> value;

            while (true) {
                uint8_t ehdr_buf[WalEntryHeader::SIZE]{};
                file.read(reinterpret_cast<char*>(ehdr_buf), WalEntryHeader::SIZE);
                const std::streamsize got = file.gcount();
                if (got == 0) { break; }
                if (got != static_cast<std::streamsize>(WalEntryHeader::SIZE)) { break; }

                const WalEntryHeader ehdr = WalEntryHeader::deserialize(ehdr_buf);
                if (!ehdr.verify_lengths(options.max_entry_bytes)) {
                    ++result.corrupt_segments;
                    break;
                }

                key.resize(ehdr.key_len);
                value.resize(ehdr.value_len);
                if (!read_exact(file, key.data(), key.size()) || !read_exact(file, value.data(), value.size())) { break; }

                const std::span<const uint8_t> key_span{key.data(), key.size()};
                const std::span<const uint8_t> value_span{value.data(), value.size()};
                if (!ehdr.verify_checksum(key_span, value_span)) {
                    ++result.corrupt_segments;
                    break;
                }

                ++result.entries_seen;
                result.max_seq = std::max(result.max_seq, ehdr.seq);

                if (ehdr.seq <= options.checkpoint_seq) { continue; }

                WalRecoveredEntry out;
                out.key = key;
                out.value = value;
                out.seq = ehdr.seq;
                out.key_fp64 = ehdr.key_fp64;
                out.flags = ehdr.flags;
                out.shard_id = segment.header.shard_id;
                out.segment_id = segment.header.segment_id;
                callback(out);
                ++result.entries_replayed;
                replayed_any = true;
            }

            if (replayed_any) { ++result.segments_replayed; }
        }
    } // namespace

    WalRecoveryResult WalRecovery::recover(const WalRecoveryOptions& options, const Callback& callback) {
        if (!callback) { throw std::invalid_argument("WAL recovery callback is empty"); }
        WalRecoveryResult result{};
        const std::vector<SegmentFile> files = list_segments(options.wal_dir, result);
        for (const SegmentFile& segment : files) { recover_segment(segment, options, callback, result); }
        return result;
    }

    WalRecoveryResult WalRecovery::recover_into(const WalRecoveryOptions& options, memtable::MemTable& memtable) {
        WalRecoveryResult result = recover(
            options,
            [&](const WalRecoveredEntry& entry) {
                memtable.put(
                    std::span<const uint8_t>{entry.key.data(), entry.key.size()},
                    std::span<const uint8_t>{entry.value.data(), entry.value.size()},
                    entry.seq,
                    static_cast<uint8_t>(entry.flags & 0xffu),
                    entry.key_fp64,
                    0
                );
            }
        );
        if (result.max_seq > 0) { memtable.advance_seq(result.max_seq); }
        return result;
    }
} // namespace akkaradb::engine::wal

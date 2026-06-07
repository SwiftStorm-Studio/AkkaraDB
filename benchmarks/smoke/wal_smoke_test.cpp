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

// benchmarks/smoke/wal_smoke_test.cpp
#include "TestErrorHandlers.hpp"

#include "akk/core/record/MemHdr16.hpp"
#include "akk/engine/memtable/MemTable.hpp"
#include "akk/engine/wal/WalFraming.hpp"
#include "akk/engine/wal/WalRecovery.hpp"
#include "akk/engine/wal/WalWriter.hpp"

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

using namespace akkaradb::engine::wal;

namespace {
    namespace fs = std::filesystem;

    static fs::path make_temp_dir(const std::string& suffix) {
        auto dir = fs::temp_directory_path() / ("akkaradb_wal_smoke_" + suffix);
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir, ec);
        assert(!ec);
        return dir;
    }

    static std::span<const uint8_t> as_bytes(std::string_view s) {
        return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
    }

    static size_t wal_file_count(const fs::path& dir) {
        size_t count = 0;
        for (const auto& entry : fs::directory_iterator(dir)) {
            if (entry.is_regular_file() && entry.path().extension() == ".akwal") { ++count; }
        }
        return count;
    }

    static fs::path first_wal_file(const fs::path& dir) {
        for (const auto& entry : fs::directory_iterator(dir)) {
            if (entry.is_regular_file() && entry.path().extension() == ".akwal") { return entry.path(); }
        }
        assert(false && "expected at least one WAL file");
        return {};
    }

    static WalEntryHeader read_entry_header_at(const fs::path& path, std::streamoff off) {
        std::ifstream file(path, std::ios::binary);
        assert(file.good());
        file.seekg(off, std::ios::beg);
        uint8_t buf[WalEntryHeader::SIZE]{};
        file.read(reinterpret_cast<char*>(buf), WalEntryHeader::SIZE);
        assert(file.gcount() == static_cast<std::streamsize>(WalEntryHeader::SIZE));
        return WalEntryHeader::deserialize(buf);
    }

    static void test_sync_recovery() {
        const auto dir = make_temp_dir("sync");
        {
            auto writer = WalWriter::create(WalOptions{.wal_dir = dir, .sync_mode = WalSyncMode::Sync, .shard_count = 1});
            writer->append(as_bytes("alpha"), as_bytes("one"), 1, akkaradb::core::MemHdr16::FLAG_NORMAL);
            writer->append(as_bytes("beta"), as_bytes("two"), 2, akkaradb::core::MemHdr16::FLAG_NORMAL);
            writer->close();
        }

        std::vector<WalRecoveredEntry> entries;
        const auto result = WalRecovery::recover(WalRecoveryOptions{.wal_dir = dir}, [&](const WalRecoveredEntry& e) { entries.push_back(e); });
        assert(result.entries_replayed == 2);
        assert(entries.size() == 2);
        assert(std::string(entries[0].key.begin(), entries[0].key.end()) == "alpha");
        assert(std::string(entries[0].value.begin(), entries[0].value.end()) == "one");
        assert(entries[1].seq == 2);
    }

    static void test_tombstone_recover_into_memtable() {
        const auto dir = make_temp_dir("tombstone");
        {
            auto writer = WalWriter::create(WalOptions{.wal_dir = dir, .sync_mode = WalSyncMode::Sync, .shard_count = 1});
            writer->append(as_bytes("dead"), as_bytes("alive"), 1, akkaradb::core::MemHdr16::FLAG_NORMAL);
            writer->append(as_bytes("dead"), std::span<const uint8_t>{}, 2, akkaradb::core::MemHdr16::FLAG_TOMBSTONE);
            writer->close();
        }

        auto memtable = akkaradb::engine::memtable::MemTable::create();
        const auto result = WalRecovery::recover_into(WalRecoveryOptions{.wal_dir = dir}, *memtable);
        assert(result.entries_replayed == 2);
        assert(result.max_seq == 2);
        assert(memtable->last_seq() == 3);

        std::vector<uint8_t> out;
        const auto found = memtable->get_into(as_bytes("dead"), 10, out);
        assert(found.has_value());
        assert(!found.value());
    }

    static void test_async_force_sync() {
        const auto dir = make_temp_dir("async");
        {
            auto writer = WalWriter::create(WalOptions{.wal_dir = dir, .sync_mode = WalSyncMode::Async, .shard_count = 2, .group_n = 8, .group_micros = 500});
            for (uint64_t i = 1; i <= 100; ++i) {
                const std::string key = "k" + std::to_string(i);
                const std::string val = "v" + std::to_string(i);
                writer->append(as_bytes(key), as_bytes(val), i, akkaradb::core::MemHdr16::FLAG_NORMAL);
            }
            writer->force_sync();
            writer->close();
        }

        uint64_t count = 0;
        const auto result = WalRecovery::recover(WalRecoveryOptions{.wal_dir = dir}, [&](const WalRecoveredEntry&) { ++count; });
        assert(result.entries_replayed == 100);
        assert(count == 100);
        assert(result.max_seq == 100);
    }

    static void test_crc_stops_segment() {
        const auto dir = make_temp_dir("crc");
        {
            auto writer = WalWriter::create(WalOptions{.wal_dir = dir, .sync_mode = WalSyncMode::Sync, .shard_count = 1});
            writer->append(as_bytes("a"), as_bytes("1"), 1, akkaradb::core::MemHdr16::FLAG_NORMAL);
            writer->append(as_bytes("b"), as_bytes("2"), 2, akkaradb::core::MemHdr16::FLAG_NORMAL);
            writer->close();
        }

        const fs::path path = first_wal_file(dir);
        const WalEntryHeader first = read_entry_header_at(path, WalSegmentHeader::SIZE);
        {
            std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
            assert(file.good());
            file.seekp(static_cast<std::streamoff>(WalSegmentHeader::SIZE + first.entry_len + WalEntryHeader::SIZE), std::ios::beg);
            char bad = '\x7f';
            file.write(&bad, 1);
        }

        uint64_t count = 0;
        const auto result = WalRecovery::recover(WalRecoveryOptions{.wal_dir = dir}, [&](const WalRecoveredEntry&) { ++count; });
        assert(count == 1);
        assert(result.entries_replayed == 1);
        assert(result.corrupt_segments >= 1);
    }

    static void test_truncated_tail() {
        const auto dir = make_temp_dir("truncate");
        {
            auto writer = WalWriter::create(WalOptions{.wal_dir = dir, .sync_mode = WalSyncMode::Sync, .shard_count = 1});
            writer->append(as_bytes("a"), as_bytes("1"), 1, akkaradb::core::MemHdr16::FLAG_NORMAL);
            writer->append(as_bytes("b"), as_bytes("2"), 2, akkaradb::core::MemHdr16::FLAG_NORMAL);
            writer->close();
        }

        const fs::path path = first_wal_file(dir);
        const WalEntryHeader first = read_entry_header_at(path, WalSegmentHeader::SIZE);
        fs::resize_file(path, WalSegmentHeader::SIZE + first.entry_len + 10);

        uint64_t count = 0;
        const auto result = WalRecovery::recover(WalRecoveryOptions{.wal_dir = dir}, [&](const WalRecoveredEntry&) { ++count; });
        assert(count == 1);
        assert(result.entries_replayed == 1);
    }

    static void test_rotation_and_prune() {
        const auto dir = make_temp_dir("rotation");
        std::vector<uint8_t> payload(1024 * 1024, static_cast<uint8_t>('x'));
        auto writer = WalWriter::create(WalOptions{.wal_dir = dir, .sync_mode = WalSyncMode::Sync, .shard_count = 1});

        for (uint64_t i = 1; i <= 70; ++i) {
            const std::string key = "rot" + std::to_string(i);
            writer->append(as_bytes(key), std::span<const uint8_t>{payload.data(), payload.size()}, i, akkaradb::core::MemHdr16::FLAG_NORMAL);
        }
        writer->force_sync();
        assert(wal_file_count(dir) >= 2);

        writer->prune_until(100);
        assert(wal_file_count(dir) == 1);
        writer->close();
    }
}

int main() {
    akkara::test::install_msvc_test_error_handlers();

    test_sync_recovery();
    test_tombstone_recover_into_memtable();
    test_async_force_sync();
    test_crc_stops_segment();
    test_truncated_tail();
    test_rotation_and_prune();
    std::printf("wal smoke test passed\n");
    return 0;
}

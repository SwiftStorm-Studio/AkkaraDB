/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// benchmarks/smoke/wal_smoke_test.cpp
#include "TestErrorHandlers.hpp"

#include "akk/core/record/MemHdr16.hpp"
#include "akk/engine/memtable/MemTable.hpp"
#include "akk/engine/wal/WalFraming.hpp"
#include "akk/engine/wal/WalRecovery.hpp"
#include "akk/engine/wal/WalWriter.hpp"

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

    static fs::path makeTempDir(const std::string& suffix) {
        auto dir = fs::temp_directory_path() / ("akkaradbWalSmoke_" + suffix);
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir, ec);
        AKK_TEST_CHECK(!ec);
        return dir;
    }

    static std::span<const uint8_t> asBytes(std::string_view s) {
        return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
    }

    static size_t walFileCount(const fs::path& dir) {
        size_t count = 0;
        for (const auto& entry : fs::directory_iterator(dir)) {
            if (entry.is_regular_file() && entry.path().extension() == ".akwal") { ++count; }
        }
        return count;
    }

    static fs::path firstWalFile(const fs::path& dir) {
        for (const auto& entry : fs::directory_iterator(dir)) {
            if (entry.is_regular_file() && entry.path().extension() == ".akwal") { return entry.path(); }
        }
        AKK_TEST_CHECK(false && "expected at least one WAL file");
        return {};
    }

    static WalEntryHeader readEntryHeaderAt(const fs::path& path, std::streamoff off) {
        std::ifstream file(path, std::ios::binary);
        AKK_TEST_CHECK(file.good());
        file.seekg(off, std::ios::beg);
        uint8_t buf[WalEntryHeader::SIZE]{};
        file.read(reinterpret_cast<char*>(buf), WalEntryHeader::SIZE);
        AKK_TEST_CHECK(file.gcount() == static_cast<std::streamsize>(WalEntryHeader::SIZE));
        return WalEntryHeader::deserialize(buf);
    }

    static void testSyncRecovery() {
        const auto dir = makeTempDir("sync");
        {
            auto writer = WalWriter::create(WalOptions{.walDir = dir, .syncMode = WalSyncMode::SYNC, .shardCount = 1});
            writer->append(asBytes("alpha"), asBytes("one"), 1, akkaradb::core::MemHdr16::FLAG_NORMAL);
            writer->append(asBytes("beta"), asBytes("two"), 2, akkaradb::core::MemHdr16::FLAG_NORMAL);
            writer->close();
        }

        std::vector<WalRecoveredEntry> entries;
        const auto result = WalRecovery::recover(WalRecoveryOptions{.walDir = dir}, [&](const WalRecoveredEntry& e) { entries.push_back(e); });
        AKK_TEST_CHECK(result.entriesReplayed == 2);
        AKK_TEST_CHECK(entries.size() == 2);
        AKK_TEST_CHECK(std::string(entries[0].key.begin(), entries[0].key.end()) == "alpha");
        AKK_TEST_CHECK(std::string(entries[0].value.begin(), entries[0].value.end()) == "one");
        AKK_TEST_CHECK(entries[1].seq == 2);
    }

    static void testTombstoneRecoverIntoMemtable() {
        const auto dir = makeTempDir("tombstone");
        {
            auto writer = WalWriter::create(WalOptions{.walDir = dir, .syncMode = WalSyncMode::SYNC, .shardCount = 1});
            writer->append(asBytes("dead"), asBytes("alive"), 1, akkaradb::core::MemHdr16::FLAG_NORMAL);
            writer->append(asBytes("dead"), std::span<const uint8_t>{}, 2, akkaradb::core::MemHdr16::FLAG_TOMBSTONE);
            writer->close();
        }

        auto memtable = akkaradb::engine::memtable::MemTable::create();
        const auto result = WalRecovery::recoverInto(WalRecoveryOptions{.walDir = dir}, *memtable);
        AKK_TEST_CHECK(result.entriesReplayed == 2);
        AKK_TEST_CHECK(result.maxSeq == 2);
        AKK_TEST_CHECK(memtable->lastSeq() == 3);

        std::vector<uint8_t> out;
        const auto found = memtable->getInto(asBytes("dead"), 10, out);
        AKK_TEST_CHECK(found.has_value());
        AKK_TEST_CHECK(!found.value());
    }

    static void testAsyncForceSync() {
        const auto dir = makeTempDir("async");
        {
            auto writer = WalWriter::create(WalOptions{.walDir = dir, .syncMode = WalSyncMode::ASYNC, .shardCount = 2, .groupN = 8, .groupMicros = 500});
            for (uint64_t i = 1; i <= 100; ++i) {
                const std::string key = "k" + std::to_string(i);
                const std::string val = "v" + std::to_string(i);
                writer->append(asBytes(key), asBytes(val), i, akkaradb::core::MemHdr16::FLAG_NORMAL);
            }
            writer->forceSync();
            writer->close();
        }

        uint64_t count = 0;
        const auto result = WalRecovery::recover(WalRecoveryOptions{.walDir = dir}, [&](const WalRecoveredEntry&) { ++count; });
        AKK_TEST_CHECK(result.entriesReplayed == 100);
        AKK_TEST_CHECK(count == 100);
        AKK_TEST_CHECK(result.maxSeq == 100);
    }

    static void testCrcStopsSegment() {
        const auto dir = makeTempDir("crc");
        {
            auto writer = WalWriter::create(WalOptions{.walDir = dir, .syncMode = WalSyncMode::SYNC, .shardCount = 1});
            writer->append(asBytes("a"), asBytes("1"), 1, akkaradb::core::MemHdr16::FLAG_NORMAL);
            writer->append(asBytes("b"), asBytes("2"), 2, akkaradb::core::MemHdr16::FLAG_NORMAL);
            writer->close();
        }

        const fs::path path = firstWalFile(dir);
        const WalEntryHeader first = readEntryHeaderAt(path, WalSegmentHeader::SIZE);
        {
            std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
            AKK_TEST_CHECK(file.good());
            file.seekp(static_cast<std::streamoff>(WalSegmentHeader::SIZE + first.entryLen + WalEntryHeader::SIZE), std::ios::beg);
            char bad = '\x7f';
            file.write(&bad, 1);
        }

        uint64_t count = 0;
        const auto result = WalRecovery::recover(WalRecoveryOptions{.walDir = dir}, [&](const WalRecoveredEntry&) { ++count; });
        AKK_TEST_CHECK(count == 1);
        AKK_TEST_CHECK(result.entriesReplayed == 1);
        AKK_TEST_CHECK(result.corruptSegments >= 1);
    }

    static void testTruncatedTail() {
        const auto dir = makeTempDir("truncate");
        {
            auto writer = WalWriter::create(WalOptions{.walDir = dir, .syncMode = WalSyncMode::SYNC, .shardCount = 1});
            writer->append(asBytes("a"), asBytes("1"), 1, akkaradb::core::MemHdr16::FLAG_NORMAL);
            writer->append(asBytes("b"), asBytes("2"), 2, akkaradb::core::MemHdr16::FLAG_NORMAL);
            writer->close();
        }

        const fs::path path = firstWalFile(dir);
        const WalEntryHeader first = readEntryHeaderAt(path, WalSegmentHeader::SIZE);
        fs::resize_file(path, WalSegmentHeader::SIZE + first.entryLen + 10);

        uint64_t count = 0;
        const auto result = WalRecovery::recover(WalRecoveryOptions{.walDir = dir}, [&](const WalRecoveredEntry&) { ++count; });
        AKK_TEST_CHECK(count == 1);
        AKK_TEST_CHECK(result.entriesReplayed == 1);
    }

    static void testRotationAndPrune() {
        const auto dir = makeTempDir("rotation");
        std::vector<uint8_t> payload(1024 * 1024, static_cast<uint8_t>('x'));
        auto writer = WalWriter::create(WalOptions{.walDir = dir, .syncMode = WalSyncMode::SYNC, .shardCount = 1});

        for (uint64_t i = 1; i <= 70; ++i) {
            const std::string key = "rot" + std::to_string(i);
            writer->append(asBytes(key), std::span<const uint8_t>{payload.data(), payload.size()}, i, akkaradb::core::MemHdr16::FLAG_NORMAL);
        }
        writer->forceSync();
        AKK_TEST_CHECK(walFileCount(dir) >= 2);

        writer->pruneUntil(100);
        AKK_TEST_CHECK(walFileCount(dir) == 1);
        writer->close();
    }
}

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();

    testSyncRecovery();
    testTombstoneRecoverIntoMemtable();
    testAsyncForceSync();
    testCrcStopsSegment();
    testTruncatedTail();
    testRotationAndPrune();
    std::printf("wal smoke test passed\n");
    return 0;
}
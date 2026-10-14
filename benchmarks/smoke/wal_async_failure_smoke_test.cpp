/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// benchmarks/smoke/wal_async_failure_smoke_test.cpp
#include "TestErrorHandlers.hpp"

#include "akk/engine/AkkEngine.hpp"
#include "akk/engine/wal/WalRecovery.hpp"
#include "akk/engine/wal/WalWriter.hpp"
#include "akk/engine/wal/WalFraming.hpp"
#include "akk/cpu/CRC32C.hpp"

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <algorithm>
#include <array>
#include <filesystem>
#include <fstream>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <vector>

#ifdef _WIN32
#include <stdlib.h>
#endif

namespace {
    namespace fs = std::filesystem;
    namespace wal = akkaradb::engine::wal;

    void require(bool condition, const char* message) {
        if (!condition) { throw std::runtime_error(message); }
    }

    [[nodiscard]] std::span<const uint8_t> bytes(const std::vector<uint8_t>& value) noexcept {
        return {value.data(), value.size()};
    }

    [[nodiscard]] fs::path findSingleWalSegment(const fs::path& dir) {
        std::optional<fs::path> found;
        for (const auto& entry : fs::directory_iterator(dir)) {
            if (!entry.is_regular_file() || entry.path().extension() != ".akwal") { continue; }
            require(!found.has_value(), "test expected a single WAL segment");
            found = entry.path();
        }
        require(found.has_value(), "test expected a WAL segment");
        return *found;
    }

    [[nodiscard]] std::vector<fs::path> findWalSegments(const fs::path& dir) {
        std::vector<fs::path> segments;
        for (const auto& entry : fs::directory_iterator(dir)) {
            if (entry.is_regular_file() && entry.path().extension() == ".akwal") { segments.push_back(entry.path()); }
        }
        std::sort(segments.begin(), segments.end());
        return segments;
    }

    void setWalFailureInjection(const char* value) {
#ifdef _WIN32
        if (_putenv_s("AKKARADB_TEST_WAL_FAIL_AFTER_ENTRIES", value) != 0) {
            throw std::runtime_error("failed to set WAL failure injection env");
        }
#else
        if (::setenv("AKKARADB_TEST_WAL_FAIL_AFTER_ENTRIES", value, 1) != 0) {
            throw std::runtime_error("failed to set WAL failure injection env");
        }
#endif
    }

    void clearWalFailureInjection() {
#ifdef _WIN32
        (void)_putenv_s("AKKARADB_TEST_WAL_FAIL_AFTER_ENTRIES", "");
#else
        (void)::unsetenv("AKKARADB_TEST_WAL_FAIL_AFTER_ENTRIES");
#endif
    }

    void setWalSegmentBytes(const char* value) {
#ifdef _WIN32
        if (_putenv_s("AKKARADB_TEST_WAL_SEGMENT_BYTES", value) != 0) {
            throw std::runtime_error("failed to set WAL segment size env");
        }
#else
        if (::setenv("AKKARADB_TEST_WAL_SEGMENT_BYTES", value, 1) != 0) {
            throw std::runtime_error("failed to set WAL segment size env");
        }
#endif
    }

    void clearWalSegmentBytes() {
#ifdef _WIN32
        (void)_putenv_s("AKKARADB_TEST_WAL_SEGMENT_BYTES", "");
#else
        (void)::unsetenv("AKKARADB_TEST_WAL_SEGMENT_BYTES");
#endif
    }

    void testAsyncRotationFailurePoisonsWriter() {
        const fs::path dir = fs::temp_directory_path() / "akkaradb_wal_async_failure_smoke";
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir, ec);
        require(!ec, "failed to create WAL failure smoke directory");

        setWalFailureInjection("4");
        try {
            wal::WalOptions options;
            options.execution = wal::WalExecutionMode::ASYNC;
            options.syncPolicy = wal::WalSyncPolicy::NEVER;
            options.backpressure = wal::WalBackpressureMode::BLOCK;
            options.shardCount = 1;
            options.groupN = 128;
            options.groupMicros = 1000;
            options.groupBytes = 4ULL * 1024ULL * 1024ULL;
            options.asyncMaxPendingBytes = 8ULL * 1024ULL * 1024ULL;

            auto writer = wal::WalWriter::create(dir, options);
            const std::vector<uint8_t> key{'w', 'a', 'l', '-', 'k'};
            const std::vector<uint8_t> value(4096, static_cast<uint8_t>('v'));

            bool appendFailed = false;
            for (uint64_t seq = 1; seq <= 16; ++seq) {
                try { writer->append(bytes(key), bytes(value), seq, 0, 0, wal::WalAppendAck::ENQUEUED); }
                catch (const std::runtime_error&) {
                    appendFailed = true;
                    break;
                }
            }

            bool syncFailed = false;
            try { writer->forceSync(); }
            catch (const std::runtime_error&) { syncFailed = true; }
            require(appendFailed || syncFailed, "async WAL rotation failure must be reported to the caller");

            const auto snap = writer->snapshot();
            require(!snap.healthy, "async WAL failure must mark the writer unhealthy");
            require(snap.asyncFailures > 0, "async WAL failure counter must be incremented");
            require(snap.pendingEntries == 0, "failed async WAL queue must be drained");
            require(snap.pendingBytes == 0, "failed async WAL queue bytes must be cleared");

            bool subsequentAppendFailed = false;
            try { writer->append(bytes(key), bytes(value), 100, 0, 0, wal::WalAppendAck::ENQUEUED); }
            catch (const std::runtime_error&) { subsequentAppendFailed = true; }
            require(subsequentAppendFailed, "poisoned async WAL writer must reject subsequent appends");
        }
        catch (...) {
            clearWalFailureInjection();
            throw;
        }
        clearWalFailureInjection();
        fs::remove_all(dir, ec);
    }

    void testRecoveryStopsAfterCorruptSegmentWithinShard() {
        const fs::path dir = fs::temp_directory_path() / "akkaradb_wal_recovery_contiguous_prefix_smoke";
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir, ec);
        require(!ec, "failed to create WAL contiguous prefix smoke directory");

        setWalSegmentBytes("120");
        try {
            wal::WalOptions options;
            options.execution = wal::WalExecutionMode::INLINE;
            options.syncPolicy = wal::WalSyncPolicy::ALWAYS;
            options.shardCount = 1;

            const std::vector<uint8_t> key1{'p', 'r', 'e', '-', '1'};
            const std::vector<uint8_t> key2{'p', 'r', 'e', '-', '2'};
            const std::vector<uint8_t> key3{'p', 'r', 'e', '-', '3'};
            const std::vector<uint8_t> value(16, static_cast<uint8_t>('v'));

            {
                auto writer = wal::WalWriter::create(dir, options);
                writer->append(bytes(key1), bytes(value), 1, 0, 0, wal::WalAppendAck::SYNCED);
                writer->append(bytes(key2), bytes(value), 2, 0, 0, wal::WalAppendAck::SYNCED);
                writer->append(bytes(key3), bytes(value), 3, 0, 0, wal::WalAppendAck::SYNCED);
                writer->close();
            }

            const auto segments = findWalSegments(dir);
            require(segments.size() >= 3, "test expected at least three WAL segments");
            {
                std::ofstream out(segments[1], std::ios::binary | std::ios::app);
                require(static_cast<bool>(out), "failed to append corrupt WAL segment tail");
                const std::vector<uint8_t> garbage{0xff, 0x7f, 0x01, 0x02, 0x03};
                out.write(reinterpret_cast<const char*>(garbage.data()), static_cast<std::streamsize>(garbage.size()));
                require(static_cast<bool>(out), "failed to write corrupt WAL segment tail");
            }

            std::vector<wal::WalRecoveredEntry> recovered;
            const auto result = wal::WalRecovery::recover(
                wal::WalRecoveryOptions{.walDir = dir},
                [&](const wal::WalRecoveredEntry& entry) { recovered.push_back(entry); }
            );

            require(result.corruptSegments > 0, "WAL recovery did not report the corrupt segment");
            require(result.entriesReplayed == 2, "WAL recovery accepted entries after a corrupt segment");
            require(recovered.size() == 2, "WAL recovery callback count for contiguous prefix mismatch");
            require(recovered[0].key == key1, "WAL recovery lost the first valid prefix entry");
            require(recovered[1].key == key2, "WAL recovery lost the second valid prefix entry");

            recovered.clear();
            const auto truncation = wal::WalRecovery::recover(
                wal::WalRecoveryOptions{.walDir = dir, .truncateCorruptTail = true},
                [&](const wal::WalRecoveredEntry& entry) { recovered.push_back(entry); }
            );
            require(truncation.corruptSegments > 0, "WAL truncation did not detect the corrupt segment");
            require(truncation.segmentsTruncated > 0, "WAL truncation did not truncate the corrupt segment");
            require(truncation.segmentsRemoved > 0, "WAL truncation did not remove later same-shard segments");
            require(truncation.entriesReplayed == 2, "WAL truncation replayed entries beyond the valid prefix");

            recovered.clear();
            const auto truncatedRecovery = wal::WalRecovery::recover(
                wal::WalRecoveryOptions{.walDir = dir},
                [&](const wal::WalRecoveredEntry& entry) { recovered.push_back(entry); }
            );
            require(truncatedRecovery.corruptSegments == 0, "WAL truncation left a corrupt segment behind");
            require(truncatedRecovery.entriesReplayed == 2, "WAL recovery after truncation did not preserve the valid prefix");
        }
        catch (...) {
            clearWalSegmentBytes();
            throw;
        }
        clearWalSegmentBytes();
        fs::remove_all(dir, ec);
    }

    void testRecoveryChecksPublishedRecordBoundary() {
        const fs::path dir = fs::temp_directory_path() / "akkaradb_wal_published_boundary_smoke";
        for (const auto execution : {wal::WalExecutionMode::INLINE, wal::WalExecutionMode::ASYNC}) {
            std::error_code ec; fs::remove_all(dir, ec); fs::create_directories(dir);
            wal::WalOptions options; options.execution = execution; options.syncPolicy = wal::WalSyncPolicy::ON_SYNC_ACK; options.shardCount = 1;
            auto writer = wal::WalWriter::create(dir, options);
            const std::vector<uint8_t> first{'a'}, second{'b'}, value{'v'};
            writer->append(first, value, 7, 0, 0, wal::WalAppendAck::SYNCED);
            const auto path = findWalSegments(dir).front();
            const auto firstBytes = fs::file_size(path);
            std::array<uint8_t, wal::WalSegmentHeader::SIZE> prefix{};
            { std::ifstream input{path, std::ios::binary}; input.read(reinterpret_cast<char*>(prefix.data()), prefix.size()); require(bool(input), "failed to read synced WAL header"); }
            // Identical sequences cannot reveal loss through a maximum-sequence check.
            writer->append(second, value, 7, 0, 0, wal::WalAppendAck::SYNCED);
            { std::array<uint8_t, wal::WalSegmentHeader::SIZE> current{};
                std::ifstream input{path, std::ios::binary}; input.read(reinterpret_cast<char*>(current.data()), current.size());
                require(wal::WalSegmentHeader::deserialize(current.data()).publishedBytes == fs::file_size(path), "SYNCED ack did not publish its complete record boundary"); }
            writer->close(); writer.reset();
            const auto complete = wal::WalRecovery::recover({.walDir = dir}, [](const auto&) {});
            require(complete.corruptSegments == 0 && complete.entriesReplayed == 2, "valid same-sequence WAL recovery failed");
            fs::resize_file(path, firstBytes);
            const auto lost = wal::WalRecovery::recover({.walDir = dir, .checkpointSeq = 7}, [](const auto&) {});
            require(lost.corruptSegments == 1, "record-aligned loss below the checkpoint was accepted");
            const auto repaired = wal::WalRecovery::recover({.walDir = dir, .truncateCorruptTail = true}, [](const auto&) {});
            require(repaired.corruptSegments == 1 && repaired.segmentsTruncated == 1, "explicit valid-prefix truncation did not update the published boundary");
            const auto clean = wal::WalRecovery::recover({.walDir = dir}, [](const auto&) {});
            require(clean.corruptSegments == 0 && clean.entriesReplayed == 1, "recovery still rejected the explicitly repaired prefix");
            // A crash can leave a stale header followed by additional valid records.
            writer = wal::WalWriter::create(dir, options); writer->append(second, value, 7, 0, 0, wal::WalAppendAck::SYNCED); writer->close(); writer.reset();
            { std::fstream output{path, std::ios::binary | std::ios::in | std::ios::out}; output.write(reinterpret_cast<const char*>(prefix.data()), prefix.size()); }
            const auto stale = wal::WalRecovery::recover({.walDir = dir}, [](const auto&) {});
            require(stale.corruptSegments == 0 && stale.entriesReplayed == 2, "valid records after a stale published boundary were rejected");
            fs::remove_all(dir, ec);
        }
    }

    void testLegacyWalRequiresRecreation() {
        const fs::path dir = fs::temp_directory_path() / "akkaradb_wal_legacy_header_smoke";
        std::error_code ec; fs::remove_all(dir, ec); fs::create_directories(dir);
        auto header = wal::WalSegmentHeader::build(0, 0, 0); header.version = 1; header.headerSize = 48; header.crc32c = 0;
        std::array<uint8_t, wal::WalSegmentHeader::SIZE> bytes{}; header.serialize(bytes.data());
        const auto crc = akkaradb::cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), 48);
        for (unsigned n = 0; n < 4; ++n) { bytes[36 + n] = static_cast<uint8_t>(crc >> (8 * n)); }
        const auto path = dir / "0000_0000000000000000.akwal";
        { std::ofstream output{path, std::ios::binary}; output.write(reinterpret_cast<const char*>(bytes.data()), 48); }
        bool rejected = false;
        try { (void)wal::WalRecovery::recover({.walDir = dir}, [](const auto&) {}); }
        catch (const std::runtime_error& error) { rejected = std::string_view{error.what()}.find("incompatible v1") != std::string_view::npos; }
        require(rejected, "legacy WAL was silently accepted");
        rejected = false;
        try { (void)wal::WalWriter::create(dir, {.execution = wal::WalExecutionMode::INLINE, .shardCount = 1}); }
        catch (const std::runtime_error&) { rejected = true; }
        require(rejected && fs::file_size(path) == 48, "legacy WAL writer did not preserve the original file");
        fs::remove_all(dir, ec);
    }

    void testWriterTruncatesCorruptTailBeforeAppending() {
        const fs::path dir = fs::temp_directory_path() / "akkaradb_wal_truncate_corrupt_tail_smoke";
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir, ec);
        require(!ec, "failed to create WAL corrupt tail smoke directory");

        wal::WalOptions options;
        options.execution = wal::WalExecutionMode::INLINE;
        options.syncPolicy = wal::WalSyncPolicy::ALWAYS;
        options.shardCount = 1;

        const std::vector<uint8_t> key1{'t', 'a', 'i', 'l', '-', '1'};
        const std::vector<uint8_t> val1{'v', 'a', 'l', '-', '1'};
        const std::vector<uint8_t> key2{'t', 'a', 'i', 'l', '-', '2'};
        const std::vector<uint8_t> val2{'v', 'a', 'l', '-', '2'};

        {
            auto writer = wal::WalWriter::create(dir, options);
            writer->append(bytes(key1), bytes(val1), 1, 0, 0, wal::WalAppendAck::SYNCED);
            writer->close();
        }

        {
            std::ofstream out(findSingleWalSegment(dir), std::ios::binary | std::ios::app);
            require(static_cast<bool>(out), "failed to append corrupt WAL tail");
            const std::vector<uint8_t> garbage{0xff, 0x7f, 0x01, 0x02, 0x03};
            out.write(reinterpret_cast<const char*>(garbage.data()), static_cast<std::streamsize>(garbage.size()));
            require(static_cast<bool>(out), "failed to write corrupt WAL tail");
        }

        {
            auto writer = wal::WalWriter::create(dir, options);
            writer->append(bytes(key2), bytes(val2), 2, 0, 0, wal::WalAppendAck::SYNCED);
            writer->close();
        }

        std::vector<wal::WalRecoveredEntry> recovered;
        const auto result = wal::WalRecovery::recover(
            wal::WalRecoveryOptions{.walDir = dir},
            [&](const wal::WalRecoveredEntry& entry) { recovered.push_back(entry); }
        );
        require(result.entriesReplayed == 2, "WAL recovery did not replay entries written around a corrupt tail");
        require(recovered.size() == 2, "WAL recovery callback count mismatch");
        require(recovered[0].key == key1 && recovered[0].value == val1, "WAL recovery lost the entry before corrupt tail");
        require(recovered[1].key == key2 && recovered[1].value == val2, "WAL recovery lost the entry after corrupt tail");

        fs::remove_all(dir, ec);
    }

    [[nodiscard]] akkaradb::engine::AkkEngineOptions asyncEnqueuedEngineOptions(const fs::path& dir) {
        namespace engine = akkaradb::engine;
        namespace memtable = engine::memtable;
        namespace wal = engine::wal;

        engine::AkkEngineOptions options;
        options.paths.dataDir = dir;
        options.components.walEnabled = true;
        options.components.blobEnabled = false;
        options.components.manifestEnabled = false;
        options.components.sstEnabled = false;
        options.components.versionLogEnabled = false;
        options.memtable.shardCount = 1;
        options.memtable.flushMode = memtable::MemTableFlushMode::MANUAL_ONLY;
        options.runtime.writeDurability = engine::AkkEngineOptions::WriteDurabilityMode::ENQUEUED;
        options.wal.execution = wal::WalExecutionMode::ASYNC;
        options.wal.syncPolicy = wal::WalSyncPolicy::NEVER;
        options.wal.shardCount = 1;
        options.wal.groupN = 1;
        return options;
    }

    template <typename Operation>
    void requireRuntimeError(Operation&& operation, const char* message) {
        try {
            operation();
        }
        catch (const std::runtime_error&) { return; }
        throw std::runtime_error(message);
    }

    void testEnginePropagatesAsyncWalFailure() {
        const fs::path dir = fs::temp_directory_path() / "akkaradb_engine_wal_async_failure_smoke";
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir, ec);
        require(!ec, "failed to create engine WAL failure smoke directory");

        setWalFailureInjection("1");
        try {
            auto engine = akkaradb::engine::AkkEngine::open(asyncEnqueuedEngineOptions(dir));
            const std::vector<uint8_t> key{'e', 'n', 'g', 'i', 'n', 'e', '-', 'k'};
            const std::vector<uint8_t> value{'e', 'n', 'g', 'i', 'n', 'e', '-', 'v'};

            engine->put(bytes(key), bytes(value));
            engine->forceSync();
            const auto durable = engine->get(bytes(key));
            require(durable.has_value() && *durable == value, "first WAL append must complete before injecting the next flusher failure");

            // This call only waits for queue admission. appendAll applies the same
            // record to the MemTable before it returns; forceSync below observes
            // the delayed flusher failure for that accepted ENQUEUED write.
            engine->put(bytes(key), bytes(value));
            requireRuntimeError([&] { engine->forceSync(); }, "engine forceSync must report the asynchronous WAL failure");
            const auto stats = engine->stats();
            require(!stats.wal.healthy, "engine stats must expose an unhealthy WAL");
            require(stats.wal.asyncFailures > 0, "engine stats must expose the asynchronous WAL failure count");
            require(stats.wal.pendingEntries == 0 && stats.wal.pendingBytes == 0, "engine stats must expose an empty failed WAL queue");

            requireRuntimeError([&] { (void)engine->get(bytes(key)); }, "engine get must reject reads after WAL failure");
            requireRuntimeError([&] { engine->put(bytes(key), bytes(value)); }, "engine put must reject writes after WAL failure");
            requireRuntimeError([&] { (void)engine->exists(bytes(key)); }, "engine exists must reject reads after WAL failure");
            requireRuntimeError([&] { engine->forceFlush(); }, "engine forceFlush must reject work after WAL failure");
            requireRuntimeError([&] { engine->close(); }, "engine close must surface the asynchronous WAL failure");
        }
        catch (...) {
            clearWalFailureInjection();
            throw;
        }
        clearWalFailureInjection();
        fs::remove_all(dir, ec);
    }
}

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();
    try {
        testAsyncRotationFailurePoisonsWriter();
        testRecoveryStopsAfterCorruptSegmentWithinShard();
        testRecoveryChecksPublishedRecordBoundary();
        testLegacyWalRequiresRecreation();
        testWriterTruncatesCorruptTailBeforeAppending();
        testEnginePropagatesAsyncWalFailure();
        return 0;
    }
    catch (const std::exception& ex) {
        std::fprintf(stderr, "wal async failure smoke failed: %s\n", ex.what());
        return 1;
    }
}

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// benchmarks/smoke/sst_snapshot_visibility_smoke_test.cpp
#include "TestErrorHandlers.hpp"

#include "akk/cpu/CRC32C.hpp"
#include "akk/core/record/KeyFingerprint.hpp"
#include "akk/core/record/RecordView.hpp"
#include "akk/engine/manifest/Manifest.hpp"
#include "akk/engine/manifest/ManifestFraming.hpp"
#include "akk/engine/sstable/SSTManager.hpp"
#include "akk/engine/sstable/SSTWriter.hpp"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <exception>
#include <chrono>
#include <thread>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#endif

namespace {
    void require(bool condition, const char* message) {
        if (!condition) { throw std::runtime_error(message); }
    }

    [[nodiscard]] std::span<const uint8_t> bytes(const std::vector<uint8_t>& value) noexcept {
        return {value.data(), value.size()};
    }

    [[nodiscard]] akkaradb::core::RecordView record(
        const std::vector<uint8_t>& key,
        const std::vector<uint8_t>& value,
        uint64_t seq,
        uint8_t flags = 0
    ) {
        const uint64_t fp64 = akkaradb::core::computeKeyFp64(key.data(), key.size());
        const uint64_t mini = akkaradb::core::buildMiniKey(key.data(), key.size());
        return {
            key.data(),
            static_cast<uint16_t>(key.size()),
            value.data(),
            static_cast<uint16_t>(value.size()),
            seq,
            flags,
            fp64,
            mini
        };
    }

    void writeSstFile(
        const std::filesystem::path& path,
        int level,
        std::span<const akkaradb::core::RecordView> records
    ) {
        akkaradb::engine::sst::SSTWriter::Options options;
        options.level = level;
        (void)akkaradb::engine::sst::SSTWriter::write(path, records, options);
    }

    void testVersionsWithinOneSst() {
        namespace fs = std::filesystem;
        namespace sst = akkaradb::engine::sst;
        const auto dir = fs::current_path() / ".bench-tmp" / "sst_repeated_key_versions";
        fs::remove_all(dir);
        fs::create_directories(dir);
        const std::vector<uint8_t> key(24, 'k'), missing{'m'}, sentinel{'!'};
        const std::vector<std::vector<uint64_t>> orders{
            {10, 20, 30, 40, 50}, {50, 40, 30, 20, 10}, {20, 10, 40, 50, 30}
        };
        for (const auto codec : {sst::SSTWriter::Codec::NONE, sst::SSTWriter::Codec::ZSTD}) {
            for (const size_t valueSize : {size_t{32}, size_t{3000}}) {
                for (const auto& order : orders) {
                    std::vector<std::vector<uint8_t>> values;
                    for (const auto seq : order) {
                        values.emplace_back(seq == 40 ? 0 : valueSize, static_cast<uint8_t>(seq));
                    }
                    std::vector<akkaradb::core::RecordView> records;
                    for (size_t i = 0; i < order.size(); ++i) {
                        records.push_back(record(key, values[i], order[i], order[i] == 40 ? sst::SST_RECORD_FLAG_TOMBSTONE : 0));
                    }
                    sst::SSTWriter::Options options;
                    options.codec = codec;
                    options.blockSize = 4096;
                    const auto path = dir / "versions.aksst";
                    (void)sst::SSTWriter::write(path, records, options);
                    auto reader = sst::SSTReader::open(path, {.blockCacheBytes = 0});
                    require(reader != nullptr, "repeated-key SST must open");
                    require((reader->header().blockCount > 1) == (valueSize == 3000), "version fixture must cover block boundaries");
                    sst::SSTManager::Options managerOptions;
                    managerOptions.compactThreads = 0;
                    managerOptions.compactionMode = sst::SSTCompactionMode::DISABLED;
                    managerOptions.codec = codec;
                    managerOptions.blockSize = options.blockSize;
                    auto manager = sst::SSTManager::create(dir / "managed", managerOptions);
                    manager->flush(records);
                    for (const uint64_t snapshot : {uint64_t{0}, uint64_t{9}, uint64_t{10}, uint64_t{19}, uint64_t{20}, uint64_t{29},
                                                  uint64_t{30}, uint64_t{39}, uint64_t{40}, uint64_t{49}, uint64_t{50}, UINT64_MAX}) {
                        const uint64_t expected = std::min<uint64_t>(snapshot / 10 * 10, 50);
                        const auto got = reader->get(key, snapshot);
                        const auto contains = reader->contains(key, snapshot);
                        auto out = sentinel;
                        const auto hit = reader->getInto(key, out, snapshot);
                        if (expected == 0) {
                            require(!got && !contains && !hit && out == sentinel, "future versions must not hide a snapshot miss");
                        }
                        else {
                            const bool live = expected != 40;
                            const std::vector<uint8_t> expectedValue(live ? valueSize : 0, static_cast<uint8_t>(expected));
                            require(got && got->seq == expected && got->value == expectedValue && got->isTombstone() == !live,
                                    "point lookup must choose newest visible version within an SST");
                            require(contains && *contains == live && hit && *hit == live, "point APIs must agree on selected version");
                            require(out == (live ? expectedValue : sentinel), "getInto must preserve output on tombstone or miss");
                        }
                        size_t count = 0;
                        for (const auto& row : reader->scan({}, {}, snapshot)) {
                            require(row.key == key && row.seq <= snapshot, "raw scan returned an invisible version");
                            ++count;
                        }
                        require(count == static_cast<size_t>(std::count_if(order.begin(), order.end(), [snapshot](uint64_t seq) {
                            return seq <= snapshot;
                        })), "raw SST scan must retain all visible physical versions");
                        auto scan = manager->scanIter({}, {}, snapshot, true);
                        const auto point = manager->get(key, snapshot);
                        const auto iteratorPoint = scan.get(key);
                        const auto row = scan.next();
                        require(point.has_value() == got.has_value() && iteratorPoint.has_value() == got.has_value() &&
                                row.has_value() == got.has_value() && !scan.hasNext(), "manager point and scan visibility must agree");
                        if (got) {
                            require(point->seq == got->seq && iteratorPoint->seq == got->seq && row->seq == got->seq &&
                                    row->flags == got->flags && row->value == got->value, "manager must merge all same-SST versions");
                        }
                    }
                    require(!reader->get(missing) && !reader->contains(missing), "absent key must remain absent");
                    manager->shutdown();
                    manager.reset();
                    fs::remove_all(dir / "managed");
                    reader.reset();
                }
            }
        }
        fs::remove_all(dir);
    }

    void testRecoveryRejectsInsufficientLevels() {
        namespace fs = std::filesystem;
        namespace sst = akkaradb::engine::sst;
        namespace manifest = akkaradb::engine::manifest;
        const auto root = fs::current_path() / ".bench-tmp" / "sst_recovery_level_limit";
        fs::remove_all(root);
        const std::vector<uint8_t> lowKey{'a'}, highKey{'z'}, value{'v'};
        for (const bool external : {false, true}) {
            const auto dir = root / (external ? "external" : "private");
            fs::create_directories(dir);
            const auto catalog = dir / (external ? "manifest.akmf" : "sst-state.akmf");
            auto mf = manifest::Manifest::create(catalog);
            writeSstFile(dir / "L0_1.aksst", 0, std::vector{record(lowKey, value, 1)});
            writeSstFile(dir / "L3_2.aksst", 3, std::vector{record(highKey, value, 2)});
            mf->sstSeal(0, "L0_1.aksst", 1, std::nullopt, std::nullopt);
            mf->sstSeal(3, "L3_2.aksst", 1, std::nullopt, std::nullopt);
            if (!external) { mf->close(); mf.reset(); }
            const auto orphan = dir / "L0_100.aksst";
            writeSstFile(orphan, 0, std::vector{record(lowKey, value, 3)});
            const auto temporary = dir / "unfinished.tmp";
            { std::ofstream file{temporary}; file << "unfinished"; }
            sst::SSTManager::Options options;
            options.compactThreads = 0;
            options.compactionMode = sst::SSTCompactionMode::DISABLED;
            options.maxLevels = 3;
            {
                auto manager = sst::SSTManager::create(dir, options, mf.get());
                bool rejected = false;
                try { manager->recover(); }
                catch (const std::runtime_error& ex) {
                    rejected = std::string{ex.what()}.find("maxLevels") != std::string::npos;
                }
                require(rejected, "recovery must reject a live SST outside maxLevels");
                require(!manager->get(lowKey) && !manager->get(highKey), "failed recovery must not publish a partial SST set");
                require(fs::exists(orphan) && fs::exists(temporary), "failed recovery must precede orphan and temporary cleanup");
                require(fs::exists(dir / "L0_1.aksst") && fs::exists(dir / "L3_2.aksst"), "failed recovery must preserve live SSTs");
                manager->shutdown();
            }
            options.maxLevels = 4;
            {
                auto manager = sst::SSTManager::create(dir, options, mf.get());
                manager->recover();
                const auto low = manager->get(lowKey);
                const auto high = manager->get(highKey);
                require(low && low->seq == 1 && high && high->seq == 2,
                        "restoring maxLevels must recover all existing data");
                require(!fs::exists(orphan) && !fs::exists(temporary), "successful recovery must still clean abandoned files");
                manager->shutdown();
            }
            if (mf) { mf->close(); }
        }
        fs::remove_all(root);
    }

    void testBlobFreeReferenceTracking() {
        namespace fs = std::filesystem;
        namespace sst = akkaradb::engine::sst;
        const auto dir = fs::current_path() / ".bench-tmp" / "sst_blob_free_tracking";
        fs::create_directories(dir);
        const std::vector<uint8_t> key{'k'}, value{'v'};
        const std::vector records{record(key, value, 1)};
        sst::SSTWriter::Options options; options.trackBlobReferences = false;
        const auto untracked = sst::SSTWriter::write(dir / "untracked.sst", records, options);
        require(untracked.entryCount == 1 && untracked.blobRefs.empty(), "Blob-free SST must omit per-key reference metadata");
        const auto tracked = sst::SSTWriter::write(dir / "tracked.sst", records);
        require(tracked.blobRefs.size() == 1, "default SST reference tracking changed");
        const std::vector blobs{record(key, value, 2, sst::SST_RECORD_FLAG_BLOB)};
        bool rejected = false;
        try { (void)sst::SSTWriter::write(dir / "invalid.sst", blobs, options); }
        catch (const std::invalid_argument&) { rejected = true; }
        require(rejected, "Blob records must reject disabled reference tracking");
        fs::remove_all(dir);
    }

    void testManifestRecoveryPrunesCompactionArtifacts() {
        namespace fs = std::filesystem;
        namespace manifest = akkaradb::engine::manifest;
        namespace sst = akkaradb::engine::sst;

        const fs::path dir = fs::current_path() / ".bench-tmp" / "sst_manifest_compaction_recovery_smoke";
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir);

        auto mf = manifest::Manifest::create(dir / "manifest.akmf");
        sst::SSTManager::Options options;
        options.compactionMode = sst::SSTCompactionMode::DISABLED;
        options.compactThreads = 0;

        const std::vector<uint8_t> alpha{'a'};
        const std::vector<uint8_t> beta{'b'};
        const std::vector<uint8_t> alphaOld{'o', 'l', 'd'};
        const std::vector<uint8_t> alphaOrphan{'o', 'r', 'p', 'h', 'a', 'n'};
        const std::vector<uint8_t> alphaNew{'n', 'e', 'w'};
        const std::vector<uint8_t> betaValue{'b', 'e', 't', 'a'};

        {
            auto manager = sst::SSTManager::create(dir, options, mf.get());
            std::vector<akkaradb::core::RecordView> records{record(alpha, alphaOld, 1)};
            manager->flush(records);
            records = {record(beta, betaValue, 2)};
            manager->flush(records);
            manager->shutdown();
        }

        const auto inputFiles = mf->liveSst();
        require(inputFiles.size() == 2, "setup must create two manifest-live input SSTs");

        const fs::path orphan = dir / "L1_999.aksst";
        std::vector<akkaradb::core::RecordView> orphanRecords{record(alpha, alphaOrphan, 3)};
        writeSstFile(orphan, 1, orphanRecords);
        require(fs::exists(orphan), "failed to create orphan compaction output");

        {
            auto recovered = sst::SSTManager::create(dir, options, mf.get());
            recovered->recover();
            require(!fs::exists(orphan), "manifest recovery must prune uncommitted compaction output");
            std::vector<uint8_t> out;
            const auto hit = recovered->getInto(bytes(alpha), out);
            require(hit.has_value() && *hit && out == alphaOld, "orphan compaction output must not affect reads");
            recovered->shutdown();
        }

        const fs::path compacted = dir / "L1_1000.aksst";
        std::vector<akkaradb::core::RecordView> compactedRecords{
            record(alpha, alphaNew, 4),
            record(beta, betaValue, 2),
        };
        writeSstFile(compacted, 1, compactedRecords);
        mf->compactionCommit({compacted.filename().string()}, inputFiles);

        {
            auto recovered = sst::SSTManager::create(dir, options, mf.get());
            recovered->recover();
            for (const auto& input : inputFiles) {
                require(!fs::exists(dir / input), "manifest recovery must prune committed compaction inputs");
            }
            require(fs::exists(compacted), "manifest recovery must keep committed compaction output");
            std::vector<uint8_t> out;
            auto hit = recovered->getInto(bytes(alpha), out);
            require(hit.has_value() && *hit && out == alphaNew, "committed compaction output must be visible after recovery");
            hit = recovered->getInto(bytes(beta), out);
            require(hit.has_value() && *hit && out == betaValue, "committed compaction output must preserve non-overwritten keys");
            recovered->shutdown();
        }

        mf->close();
        fs::remove_all(dir, ec);
    }

    void testManifestCompactPreservesReplayState() {
        namespace fs = std::filesystem;
        namespace manifest = akkaradb::engine::manifest;
        using BlobRefEntry = manifest::Manifest::SSTBlobRefsEvent::Entry;

        const fs::path dir = fs::current_path() / ".bench-tmp" / "manifest_compact_replay_state_smoke";
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir);
        const fs::path manifestPath = dir / "manifest.akmf";

        {
            auto mf = manifest::Manifest::create(manifestPath);
            mf->advance(7);
            mf->sstSeal(0, "L0_1.aksst", 1, std::optional<std::string>{"61"}, std::optional<std::string>{"61"});
            mf->sstSeal(0, "L0_2.aksst", 1, std::optional<std::string>{"62"}, std::optional<std::string>{"62"});
            mf->sstBlobRefs(
                "L0_1.aksst",
                {
                    BlobRefEntry{{'a'}, 1, akkaradb::engine::sst::SST_RECORD_FLAG_BLOB, 101},
                    BlobRefEntry{{'a'}, 2, 0, std::nullopt},
                    BlobRefEntry{{'b'}, 3, akkaradb::engine::sst::SST_RECORD_FLAG_BLOB, 102},
                }
            );
            mf->sstBlobRefs("L0_2.aksst", {BlobRefEntry{{'c'}, 4, akkaradb::core::RecordView::FLAG_TOMBSTONE, std::nullopt}});
            mf->compactionCommit({"L1_3.aksst"}, {"L0_1.aksst", "L0_2.aksst"});
            mf->sstBlobRefs(
                "L1_3.aksst",
                {
                    BlobRefEntry{{'a'}, 2, 0, std::nullopt},
                    BlobRefEntry{{'b'}, 3, akkaradb::engine::sst::SST_RECORD_FLAG_BLOB, 102},
                    BlobRefEntry{{'c'}, 4, akkaradb::core::RecordView::FLAG_TOMBSTONE, std::nullopt},
                }
            );
            mf->checkpoint(std::optional<std::string>{"compact-source"}, std::nullopt, 9);
            mf->sstDelete("L9_deleted.aksst");
            mf->nodeJoin(1, 20401, "node-one.local");
            mf->nodeJoin(2, 20402, "node-two.local");
            mf->nodeLeave(2);
            mf->primaryLease(1, 123456789);
            mf->blobPut(101, 4096, 1024, 0x12345678u, 1);
            mf->blobPut(102, 2048, 2048, 0x87654321u, 0);
            mf->blobDelete(101);
            mf->compact();
            mf->close();
        }

        {
            auto recovered = manifest::Manifest::create(manifestPath);
            const auto live = recovered->liveSst();
            require(live.size() == 1 && live[0] == "L1_3.aksst", "compacted manifest did not preserve live SST set");
            const auto deleted = recovered->deletedSst();
            require(deleted.size() == 1 && deleted[0] == "L9_deleted.aksst", "compacted manifest did not preserve deleted SST set");
            const auto checkpoint = recovered->lastCheckpoint();
            require(
                checkpoint.has_value() && checkpoint->lastSeq.has_value() && *checkpoint->lastSeq == 9,
                "compacted manifest did not preserve checkpoint"
            );
            require(recovered->stripesWritten() == 7, "compacted manifest did not preserve stripe counter");
            require(recovered->nodeJoins().size() == 2, "compacted manifest did not preserve node joins");
            require(recovered->nodeLeaves().size() == 1, "compacted manifest did not preserve node leaves");
            const auto lease = recovered->lastPrimaryLease();
            require(
                lease.has_value() && lease->nodeId == 1 && lease->leaseUntilUs == 123456789,
                "compacted manifest did not preserve primary lease"
            );
            auto liveBlobs = recovered->liveBlobs();
            std::sort(liveBlobs.begin(), liveBlobs.end());
            require(liveBlobs.size() == 1 && liveBlobs[0] == 102, "compacted manifest did not preserve live blob set");
            auto deletedBlobs = recovered->deletedBlobs();
            std::sort(deletedBlobs.begin(), deletedBlobs.end());
            require(deletedBlobs.size() == 1 && deletedBlobs[0] == 101, "compacted manifest did not preserve deleted blob set");
            const auto blobPuts = recovered->blobPuts();
            require(
                blobPuts.size() == 1 && blobPuts[0].blobId == 102 && blobPuts[0].totalSize == 2048 &&
                    blobPuts[0].storedSize == 2048 && blobPuts[0].contentCrc32c == 0x87654321u && blobPuts[0].codec == 0,
                "compacted manifest did not preserve live blob metadata"
            );
            require(recovered->sstBlobRefsComplete(), "compacted manifest did not preserve complete SST blob refs");
            const auto sstBlobRefs = recovered->sstReferencedBlobs();
            require(sstBlobRefs.size() == 1 && sstBlobRefs[0] == 102, "compacted manifest did not preserve live SST blob refs");
            recovered->close();
        }

        fs::remove_all(dir, ec);
    }

    void testZstdCompressionLevel() {
        namespace fs = std::filesystem;
        namespace sst = akkaradb::engine::sst;

        const fs::path dir = fs::current_path() / ".bench-tmp" / "sst_zstd_compression_level_smoke";
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir);

        const std::vector<uint8_t> key{'k'};
        const std::vector<uint8_t> value(4096, 'v');
        const std::vector<akkaradb::core::RecordView> records{record(key, value, 1)};

        sst::SSTWriter::Options writerOptions;
        writerOptions.zstdCompressionLevel = 3;
        const auto path = dir / "configured.aksst";
        (void)sst::SSTWriter::write(path, records, writerOptions);
        auto reader = sst::SSTReader::open(path);
        require(reader != nullptr, "configured Zstd level must produce a readable SST");
        std::vector<uint8_t> out;
        const auto hit = reader->getInto(bytes(key), out);
        require(hit.has_value() && *hit && out == value, "configured Zstd level must preserve values");

        sst::SSTManager::Options managerOptions;
        managerOptions.compactionMode = sst::SSTCompactionMode::DISABLED;
        managerOptions.compactThreads = 0;
        managerOptions.zstdCompressionLevel = std::numeric_limits<int>::max();
        bool rejected = false;
        try {
            (void)sst::SSTManager::create(dir, managerOptions);
        }
        catch (const std::invalid_argument&) {
            rejected = true;
        }
        require(rejected, "invalid Zstd level must be rejected when SST manager starts");

        reader.reset();
        fs::remove_all(dir, ec);
    }

    void testL0RecoveryUsesNumericFileOrder() {
        namespace fs = std::filesystem;
        namespace manifest = akkaradb::engine::manifest;
        namespace sst = akkaradb::engine::sst;

        const fs::path dir = fs::current_path() / ".bench-tmp" / "sst_l0_numeric_recovery_smoke";
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir);

        auto mf = manifest::Manifest::create(dir / "manifest.akmf");
        sst::SSTManager::Options options;
        options.compactionMode = sst::SSTCompactionMode::DISABLED;
        options.compactThreads = 0;

        const std::vector<uint8_t> key{'k'};
        {
            auto manager = sst::SSTManager::create(dir, options, mf.get());
            for (uint64_t seq = 1; seq <= 10; ++seq) {
                const std::vector<uint8_t> value{'v', static_cast<uint8_t>(seq)};
                const std::vector<akkaradb::core::RecordView> records{record(key, value, seq)};
                manager->flush(records);
            }
            manager->shutdown();
        }

        {
            auto recovered = sst::SSTManager::create(dir, options, mf.get());
            recovered->recover();
            std::vector<uint8_t> out;
            const auto hit = recovered->getInto(bytes(key), out);
            require(
                hit.has_value() && *hit && out == std::vector<uint8_t>({'v', 10}),
                "recovered L0 lookup must prefer the largest numeric file id"
            );
            recovered->shutdown();
        }

        mf->close();
        fs::remove_all(dir, ec);
    }

    [[nodiscard]] akkaradb::engine::sst::SSTFileHeaderV1 readSstHeader(const std::filesystem::path& path) {
        akkaradb::engine::sst::SSTFileHeaderV1 header{};
        std::ifstream in(path, std::ios::binary);
        require(static_cast<bool>(in), "failed to open SST for header read");
        in.read(reinterpret_cast<char*>(&header), sizeof(header));
        require(static_cast<bool>(in), "failed to read SST header");
        return header;
    }

    void flipFileByte(const std::filesystem::path& path, uint64_t offset) {
        std::fstream file(path, std::ios::binary | std::ios::in | std::ios::out);
        require(static_cast<bool>(file), "failed to open SST for corruption test");
        char value = 0;
        file.seekg(static_cast<std::streamoff>(offset));
        file.read(&value, 1);
        require(static_cast<bool>(file), "failed to read SST corruption target");
        value = static_cast<char>(static_cast<unsigned char>(value) ^ 0x5au);
        file.seekp(static_cast<std::streamoff>(offset));
        file.write(&value, 1);
        file.flush();
        require(static_cast<bool>(file), "failed to write SST corruption target");
    }

    void removeSstMetadataChecksums(const std::filesystem::path& path) {
        namespace sst = akkaradb::engine::sst;

        std::fstream file(path, std::ios::binary | std::ios::in | std::ios::out);
        require(static_cast<bool>(file), "failed to open SST for legacy-format test");
        sst::SSTFileHeaderV1 header{};
        file.read(reinterpret_cast<char*>(&header), sizeof(header));
        require(static_cast<bool>(file), "failed to read SST header for legacy-format test");

        sst::SSTFooterV1 footer{};
        file.seekg(static_cast<std::streamoff>(header.footerOffset));
        file.read(reinterpret_cast<char*>(&footer), sizeof(footer));
        require(static_cast<bool>(file), "failed to read SST footer for legacy-format test");

        header.flags &= ~sst::SST_FILE_FLAG_METADATA_CRC;
        std::memset(header.reserved, 0, sizeof(header.reserved));
        header.crc32c = 0;
        header.crc32c = akkaradb::cpu::CRC32C(reinterpret_cast<const std::byte*>(&header), sizeof(header));
        footer.headerCrc32c = header.crc32c;
        footer.footerCrc32c = 0;
        footer.footerCrc32c = akkaradb::cpu::CRC32C(reinterpret_cast<const std::byte*>(&footer), sizeof(footer));

        file.seekp(0);
        file.write(reinterpret_cast<const char*>(&header), sizeof(header));
        file.seekp(static_cast<std::streamoff>(header.footerOffset));
        file.write(reinterpret_cast<const char*>(&footer), sizeof(footer));
        file.flush();
        require(static_cast<bool>(file), "failed to write SST legacy-format test fixture");
    }

    template <typename Predicate>
    void waitForSst(Predicate ready) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{10};
        while (!ready()) {
            require(std::chrono::steady_clock::now() < deadline, "SST operation did not reach the expected boundary");
            std::this_thread::sleep_for(std::chrono::milliseconds{2});
        }
    }

    void verifySelectedVersion(akkaradb::engine::sst::SSTManager& manager, const std::vector<uint8_t>& key,
        uint64_t snapshot, uint64_t seq, bool tombstone, const std::vector<uint8_t>& value) {
        const auto actual = manager.get(key, snapshot);
        require(actual && actual->seq == seq && actual->isTombstone() == tombstone, "point read selected the wrong retained sequence");
        std::vector<uint8_t> into{'s', 'e', 'n', 't', 'i', 'n', 'e', 'l'};
        const auto hit = manager.getInto(key, into, snapshot);
        require(hit && *hit == !tombstone, "getInto visibility differs from point read");
        if (!tombstone) { require(actual->value == value && into == value, "getInto selected the wrong value"); }
        else { require(into == std::vector<uint8_t>({'s', 'e', 'n', 't', 'i', 'n', 'e', 'l'}), "tombstone getInto modified output"); }
        const auto contains = manager.contains(key, snapshot);
        require(contains && *contains == !tombstone, "contains visibility differs from point read");
        auto scan = manager.scanIter({}, {}, snapshot);
        const auto pinned = scan.get(key);
        require(pinned && pinned->seq == seq, "pinned point read differs from manager point read");
        bool found = false;
        while (scan.hasNext()) {
            auto row = scan.next();
            if (row->key == key) { require(!tombstone && row->seq == seq && row->value == value, "scan differs from point read"); found = true; }
        }
        require(found == !tombstone, "scan tombstone visibility differs from point read");
    }

    akkaradb::core::ArenaGenerator<akkaradb::core::RecordView> delayedFlush(
        akkaradb::core::RecordView row, std::atomic<bool>& entered, std::atomic<bool>& released) {
        entered.store(true, std::memory_order_release);
        while (!released.load(std::memory_order_acquire)) { std::this_thread::yield(); }
        co_yield row;
    }

    void testOutOfOrderSstVersions() {
        namespace fs = std::filesystem; namespace sst = akkaradb::engine::sst; namespace mf = akkaradb::engine::manifest;
        const auto root = fs::current_path() / ".bench-tmp" / "sst_out_of_order_versions";
        std::error_code ec; fs::remove_all(root, ec); fs::create_directories(root);
        const std::vector<uint8_t> key{'k'}, old{'o'}, latest{'n'}, empty;
        for (const bool external : {false, true}) {
            const auto dir = root / (external ? "external" : "standalone"); fs::create_directories(dir);
            std::unique_ptr<mf::Manifest> manifest;
            if (external) { manifest = mf::Manifest::create(dir / "external.akmf", true); manifest->start(); }
            sst::SSTManager::Options options; options.compactionMode = sst::SSTCompactionMode::DISABLED;
            auto manager = sst::SSTManager::create(dir, options, manifest.get());
            std::atomic<bool> entered{false}, released{false}, finished{false}; std::exception_ptr failure;
            std::thread older([&] {
                try { manager->flush(1, delayedFlush(record(key, old, 10), entered, released)); }
                catch (...) { failure = std::current_exception(); }
                finished.store(true, std::memory_order_release);
            });
            try {
                waitForSst([&] { return entered.load(std::memory_order_acquire) || finished.load(std::memory_order_acquire); });
                if (!entered.load()) { older.join(); if (failure) { std::rethrow_exception(failure); } }
                const std::vector rows{record(key, latest, 20)}; manager->flush(rows);
            }
            catch (...) { released.store(true); if (older.joinable()) { older.join(); } throw; }
            released.store(true, std::memory_order_release); if (older.joinable()) { older.join(); }
            if (failure) { std::rethrow_exception(failure); }
            verifySelectedVersion(*manager, key, UINT64_MAX, 20, false, latest);
            verifySelectedVersion(*manager, key, 10, 10, false, old);
            const std::vector tombstones{record(key, empty, 30, akkaradb::core::RecordView::FLAG_TOMBSTONE)}; manager->flush(tombstones);
            const std::vector delayed{record(key, old, 25)}; manager->flush(delayed);
            for (int pass = 0; pass < 2; ++pass) {
                verifySelectedVersion(*manager, key, UINT64_MAX, 30, true, empty);
                verifySelectedVersion(*manager, key, 25, 25, false, old);
                verifySelectedVersion(*manager, key, 20, 20, false, latest);
                manager->shutdown(); manager.reset();
                if (pass == 0) { manager = sst::SSTManager::create(dir, options, manifest.get()); manager->recover(); }
            }
            if (manifest) { manifest->close(); }
        }
        fs::remove_all(root, ec);
    }

    void testLateFlushAgainstCompactedVersions() {
        namespace fs = std::filesystem; namespace sst = akkaradb::engine::sst;
        const auto dir = fs::current_path() / ".bench-tmp" / "sst_late_flush_compacted_versions";
        std::error_code ec; fs::remove_all(dir, ec); fs::create_directories(dir);
        const std::vector<uint8_t> key{'k'}, filler{'z'}, old{'o'}, latest{'n'}, empty;
        sst::SSTManager::Options options;
        options.compactionMode = sst::SSTCompactionMode::BACKGROUND; options.compactThreads = 1;
        options.maxLevels = 3; options.maxL0Files = 2; options.l1MaxBytes = 1;
        auto manager = sst::SSTManager::create(dir, options);
        const std::vector first{record(key, latest, 20)}, second{record(filler, old, 1)};
        manager->flush(first); manager->flush(second);
        const auto waitBottom = [&] {
            waitForSst([&] {
                manager->throwIfBackgroundFailed(); const auto levels = manager->levelStats();
                return levels[0].fileCount == 0 && levels[1].fileCount == 0 && levels[2].fileCount != 0;
            });
        };
        waitBottom();
        const std::vector delayed{record(key, old, 10)}; manager->flush(delayed);
        verifySelectedVersion(*manager, key, UINT64_MAX, 20, false, latest);
        const std::vector tombstone{record(key, empty, 30, akkaradb::core::RecordView::FLAG_TOMBSTONE)}; manager->flush(tombstone);
        waitBottom();
        const std::vector evenLater{record(key, old, 25)}; manager->flush(evenLater);
        verifySelectedVersion(*manager, key, UINT64_MAX, 30, true, empty);
        manager->shutdown(); manager.reset();
        options.compactionMode = sst::SSTCompactionMode::DISABLED;
        manager = sst::SSTManager::create(dir, options); manager->recover();
        verifySelectedVersion(*manager, key, UINT64_MAX, 30, true, empty);
        manager->shutdown(); manager.reset(); fs::remove_all(dir, ec);
    }

    void testMissingSstCatalogFailsClosed() {
        namespace fs = std::filesystem; namespace sst = akkaradb::engine::sst; namespace mf = akkaradb::engine::manifest;
        const auto root = fs::current_path() / ".bench-tmp" / "sst_catalog_required";
        std::error_code ec; fs::remove_all(root, ec); fs::create_directories(root);
        const std::vector<uint8_t> key{'k'}, value{'v'}; const std::vector rows{record(key, value, 1)};
        writeSstFile(root / "L0_1.aksst", 0, rows);
        sst::SSTManager::Options options; options.compactionMode = sst::SSTCompactionMode::DISABLED;
        bool rejected = false;
        try { (void)sst::SSTManager::create(root, options); } catch (const std::runtime_error&) { rejected = true; }
        require(rejected && fs::exists(root / "L0_1.aksst") && !fs::exists(root / "sst-state.akmf"), "missing catalog must not import or delete unknown SSTs");
        const auto standalone = root / "standalone";
        { auto manager = sst::SSTManager::create(standalone, options); manager->flush(rows); }
        auto external = mf::Manifest::create(root / "external.akmf"); rejected = false;
        try { (void)sst::SSTManager::create(standalone, options, external.get()); } catch (const std::runtime_error&) { rejected = true; }
        require(rejected && fs::exists(standalone / "L0_1.aksst"), "changing catalog ownership must not discard live SSTs");
        external->close(); external.reset(); fs::remove_all(root, ec);
    }

    void setSstCrashPoint(const std::string& point) {
#ifdef _WIN32
        require(_putenv_s("AKKARADB_TEST_CRASH_POINT", point.c_str()) == 0, "failed to set SST crash point");
#else
        require(::setenv("AKKARADB_TEST_CRASH_POINT", point.c_str(), 1) == 0, "failed to set SST crash point");
#endif
    }

    int runSstLifecycleCrashChild(const std::string& mode, const std::string& point, const std::filesystem::path& dir) {
        namespace sst = akkaradb::engine::sst; namespace mf = akkaradb::engine::manifest;
        std::unique_ptr<mf::Manifest> manifest;
        if (mode == "fast") {
            // Deliberately leave batching stopped. A lifecycle handoff must persist
            // its preceding records without depending on the flusher's scheduling.
            manifest = mf::Manifest::create(dir / "external.akmf", true);
        }
        const std::vector<uint8_t> a{'a'}, b{'b'}, c{'c'}, d{'d'}, old{'o'}, newer{'n'}, kept{'c'}, added{'d'}, empty;
        sst::SSTManager::Options options;
        options.compactionMode = sst::SSTCompactionMode::BACKGROUND; options.compactThreads = 1;
        options.maxLevels = 2; options.maxL0Files = 1;
        if (point.starts_with("sst.flush.")) {
            options.compactionMode = sst::SSTCompactionMode::DISABLED;
            auto manager = sst::SSTManager::create(dir, options, manifest.get());
            setSstCrashPoint(point);
            const std::vector rows{record(a, old, 10), record(b, old, 11), record(c, kept, 12)};
            manager->flush(rows);
        }
        else {
            auto manager = sst::SSTManager::create(dir, options, manifest.get());
            const std::vector seed{record(a, old, 10), record(b, old, 11), record(c, kept, 12)};
            manager->flush(seed);
            waitForSst([&] { manager->throwIfBackgroundFailed(); return manager->compactionSnapshot().compactionsCompleted == 1; });
            manager->shutdown(); manager.reset();
            options.maxL0Files = 2; options.targetFileSize = 1;
            manager = sst::SSTManager::create(dir, options, manifest.get()); manager->recover();
            setSstCrashPoint(point);
            const std::vector update{record(a, empty, 20, akkaradb::core::RecordView::FLAG_TOMBSTONE), record(b, newer, 21)};
            const std::vector insertion{record(d, added, 22)};
            manager->flush(update); manager->flush(insertion);
            waitForSst([&] { manager->throwIfBackgroundFailed(); return manager->compactionSnapshot().compactionsCompleted != 0; });
        }
        throw std::runtime_error("SST child missed its requested crash boundary");
    }

    void testSstLifecycleCrashRecovery(const std::filesystem::path& executable) {
        namespace fs = std::filesystem; namespace sst = akkaradb::engine::sst; namespace mf = akkaradb::engine::manifest;
        const auto root = fs::current_path() / ".bench-tmp" / "sst_lifecycle_crashes";
        std::error_code ec; fs::remove_all(root, ec); fs::create_directories(root);
        const std::vector<uint8_t> a{'a'}, b{'b'}, c{'c'}, d{'d'}, old{'o'}, newer{'n'}, kept{'c'}, added{'d'}, empty;
        for (const std::string mode : {"standalone", "fast"}) {
            for (const std::string point : {"sst.flush.after_rename", "sst.flush.after_manifest_seal",
                "sst.compaction.after_output_tmp_write", "sst.compaction.after_output_rename", "sst.compaction.after_manifest_commit",
                "sst.compaction.after_first_input_delete", "sst.compaction.after_input_delete"}) {
                const auto dir = root / (mode + "-" + point); fs::create_directories(dir);
                const std::string args = "\"" + executable.string() + "\" --sst-lifecycle-crash-child " + mode + " " + point + " \"" + dir.string() + "\"";
#ifdef _WIN32
                const int status = std::system(("\"" + args + "\"").c_str());
                require(status == 86, "SST child did not stop at the requested lifecycle boundary");
#else
                const int status = std::system(args.c_str());
                require(status == (86 << 8), "SST child did not stop at the requested lifecycle boundary");
#endif
                if (point == "sst.compaction.after_first_input_delete") {
                    size_t l0 = 0;
                    for (const auto& entry : fs::directory_iterator(dir)) { if (entry.path().filename().string().starts_with("L0_")) { ++l0; } }
                    require(l0 == 1, "partial-deletion fixture must really delete one input and leave the other");
                }
                const bool flushOnly = point.starts_with("sst.flush.");
                const bool sealed = point != "sst.flush.after_rename";
                const bool committed = point == "sst.compaction.after_manifest_commit" || point.ends_with("input_delete");
                sst::SSTManager::Options options; options.maxLevels = 2; options.compactionMode = sst::SSTCompactionMode::DISABLED;
                for (int pass = 0; pass < 2; ++pass) {
                    std::unique_ptr<mf::Manifest> manifest;
                    if (mode == "fast") { manifest = mf::Manifest::create(dir / "external.akmf"); }
                    auto manager = sst::SSTManager::create(dir, options, manifest.get()); manager->recover();
                    if (flushOnly && !sealed) {
                        require(!manager->get(a) && !manager->get(b) && !manager->get(c), "unsealed SST must not become live after a crash");
                        require(!manager->scanIter().hasNext(), "unsealed SST leaked into a scan");
                    }
                    else if (flushOnly) {
                        verifySelectedVersion(*manager, a, UINT64_MAX, 10, false, old);
                        verifySelectedVersion(*manager, b, UINT64_MAX, 11, false, old);
                        verifySelectedVersion(*manager, c, UINT64_MAX, 12, false, kept);
                    }
                    else {
                        verifySelectedVersion(*manager, a, UINT64_MAX, 20, true, empty);
                        verifySelectedVersion(*manager, b, UINT64_MAX, 21, false, newer);
                        verifySelectedVersion(*manager, c, UINT64_MAX, 12, false, kept);
                        verifySelectedVersion(*manager, d, UINT64_MAX, 22, false, added);
                        const auto levels = manager->levelStats();
                        require(levels[0].fileCount == (committed ? 0u : 2u) && levels[1].fileCount == (committed ? 4u : 1u),
                            "recovery selected a partially replaced compaction set");
                    }
                    for (const auto& entry : fs::directory_iterator(dir)) {
                        require(entry.path().extension() != ".tmp", "SST recovery left an interrupted output temporary file");
                    }
                    manager->shutdown(); manager.reset(); if (manifest) { manifest->close(); }
                }
            }
        }
        fs::remove_all(root, ec);
    }

    void testManifestV1LargeRecords() {
        namespace mf = akkaradb::engine::manifest;
        namespace fs = std::filesystem;
        const fs::path dir = fs::current_path() / ".bench-tmp" / "manifest_v1_large_records";
        std::error_code ec;
        fs::remove_all(dir, ec); fs::create_directories(dir);
        std::vector<std::string> inputs, outputs;
        for (size_t i = 0; i < 300; ++i) {
            inputs.push_back("old-" + std::to_string(i) + std::string(240, 'i'));
            outputs.push_back("new-" + std::to_string(i) + std::string(240, 'o'));
        }
        const auto path = dir / "manifest.akmf";
        uint64_t beforeCommit = 0;
        {
            auto manifest = mf::Manifest::create(path);
            manifest->sstSeal(0, inputs.front(), 1, std::nullopt, std::nullopt);
            manifest->compactionCommit(inputs, {});
            beforeCommit = fs::file_size(path);
            manifest->compactionCommit(outputs, inputs);
            manifest->close();
        }
        const auto partial = dir / "partial.akmf";
        fs::copy_file(path, partial);
        fs::resize_file(partial, fs::file_size(partial) - 1);
        {
            auto manifest = mf::Manifest::create(partial);
            auto actual = manifest->liveSst(); auto expected = inputs;
            std::sort(actual.begin(), actual.end()); std::sort(expected.begin(), expected.end());
            require(actual == expected, "torn large compaction commit must retain all original inputs");
            manifest->compactionCommit(outputs, inputs);
            manifest->close();
        }
        {
            auto manifest = mf::Manifest::create(partial);
            auto actual = manifest->liveSst(); auto expected = outputs;
            std::sort(actual.begin(), actual.end()); std::sort(expected.begin(), expected.end());
            require(actual == expected, "records appended after tail repair must survive another reopen");
            manifest->close();
        }
        require(fs::file_size(path) - beforeCommit > UINT16_MAX, "large commit fixture must exceed the old narrow record limit");
        for (int pass = 0; pass < 2; ++pass) {
            auto manifest = mf::Manifest::create(path);
            auto actual = manifest->liveSst(); auto expected = outputs;
            std::sort(actual.begin(), actual.end()); std::sort(expected.begin(), expected.end());
            require(actual == expected, "v1 must retain every compaction output and retire every input");
            if (pass == 0) { manifest->compact(); }
            manifest->close();
        }
        std::vector<mf::SSTBlobRefEntry> refs(65536);
        for (size_t i = 0; i < refs.size(); ++i) { refs[i] = {{'k'}, i + 1, 0, std::nullopt}; }
        auto encoded = mf::encodeSstBlobRefs(7, "large.aksst", refs);
        mf::DecodedSSTBlobRefs decoded;
        require(mf::decodeSstBlobRefs(encoded.data(), static_cast<uint32_t>(encoded.size()), decoded) && decoded.entries.size() == refs.size() &&
            decoded.entries.back().seq == refs.back().seq, "v1 must support more than 65535 blob-reference entries");
        auto commit = mf::encodeCompactionCommit(1, outputs, inputs);
        mf::DecodedCompactionCommit decodedCommit;
        require(!mf::decodeCompactionCommit(commit.data(), static_cast<uint32_t>(commit.size() - 1), decodedCommit), "truncated file list must be rejected");
        commit.push_back(0);
        require(!mf::decodeCompactionCommit(commit.data(), static_cast<uint32_t>(commit.size()), decodedCommit), "trailing commit data must be rejected");
        commit[8] = commit[9] = commit[10] = commit[11] = 0xff;
        require(!mf::decodeCompactionCommit(commit.data(), static_cast<uint32_t>(commit.size()), decodedCommit), "impossible collection counts must be rejected");
        auto header = mf::ManifestRecordHeader::build(mf::ManifestRecordType::COMPACTION_COMMIT, encoded.data(), static_cast<uint32_t>(encoded.size()));
        require(header.verifyChecksum(), "v1 record header checksum must round trip");
        ++header.payloadLen;
        require(!header.verifyChecksum(), "record length corruption must be detected before allocation");
        fs::remove_all(dir, ec);
    }

    void testLargeSstCompactionRoundTrip() {
        namespace mf = akkaradb::engine::manifest;
        namespace sst = akkaradb::engine::sst;
        namespace fs = std::filesystem;
        const auto dir = fs::current_path() / ".bench-tmp" / "sst_large_compaction";
        std::error_code ec; fs::remove_all(dir, ec); fs::create_directories(dir);
        std::vector<std::vector<uint8_t>> keys;
        const std::vector<uint8_t> value{'v'};
        for (size_t i = 0; i < 300; ++i) {
            const auto key = "large-compaction-" + std::to_string(1000 + i);
            keys.emplace_back(key.begin(), key.end());
        }
        sst::SSTManager::Options options;
        options.compactionMode = sst::SSTCompactionMode::BACKGROUND;
        options.compactThreads = 1; options.maxL0Files = 2; options.targetFileSize = 1;
        {
            auto manifest = mf::Manifest::create(dir / "manifest.akmf", true); manifest->start();
            auto manager = sst::SSTManager::create(dir, options, manifest.get());
            for (size_t batch = 0; batch < 2; ++batch) {
                std::vector<akkaradb::core::RecordView> rows;
                for (size_t i = batch * 150; i < (batch + 1) * 150; ++i) { rows.push_back(record(keys[i], value, i + 1)); }
                manager->flush(rows);
            }
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{30};
            while (manager->compactionSnapshot().compactionsCompleted == 0 && std::chrono::steady_clock::now() < deadline) {
                manager->throwIfBackgroundFailed(); std::this_thread::sleep_for(std::chrono::milliseconds{5});
            }
            manager->shutdown(); manager->throwIfBackgroundFailed();
            require(manager->levelStats()[1].fileCount == 300, "fixture must produce more than 255 real compaction outputs");
            manifest->compact(); manager.reset(); manifest->close();
        }
        options.compactionMode = sst::SSTCompactionMode::DISABLED;
        {
            auto manifest = mf::Manifest::create(dir / "manifest.akmf");
            auto manager = sst::SSTManager::create(dir, options, manifest.get()); manager->recover();
            require(manager->levelStats()[0].fileCount == 0 && manager->levelStats()[1].fileCount == 300,
                "recovery must install every output without resurrecting compacted inputs");
            for (const auto& key : keys) {
                const auto actual = manager->get(key);
                require(actual && actual->value == value, "large compaction lost a key after restart");
            }
            size_t count = 0; auto scan = manager->scanIter();
            while (scan.hasNext()) { (void)scan.next(); ++count; }
            require(count == keys.size(), "large compaction scan must match point reads");
            manager->shutdown(); manager.reset(); manifest->close();
        }
        fs::remove_all(dir, ec);
    }

    void testManifestV1LongKeysAndVersionRejection() {
        namespace mf = akkaradb::engine::manifest;
        namespace sst = akkaradb::engine::sst;
        namespace fs = std::filesystem;
        const fs::path root = fs::current_path() / ".bench-tmp" / "manifest_v1_long_keys";
        std::error_code ec; fs::remove_all(root, ec); fs::create_directories(root);
        for (const bool fast : {false, true}) {
            const auto dir = root / (fast ? "fast" : "sync"); fs::create_directories(dir);
            const auto path = dir / "manifest.akmf";
            sst::SSTManager::Options options; options.compactionMode = sst::SSTCompactionMode::DISABLED;
            const std::vector<uint8_t> value{'v'};
            for (int pass = 0; pass < 2; ++pass) {
                auto manifest = mf::Manifest::create(path, fast); manifest->start();
                auto manager = sst::SSTManager::create(dir, options, manifest.get());
                manager->recover();
                for (const size_t length : {20000u, 32768u, 65534u}) {
                    const std::vector<uint8_t> key(length, 'k');
                    if (pass == 0) {
                        const std::vector<akkaradb::core::RecordView> rows{record(key, value, length)};
                        manager->flush(rows);
                    }
                    const auto actual = manager->get(key);
                    require(actual && actual->value == value, "long legal keys must survive SST/Manifest reopen");
                }
                if (pass == 0) { manifest->compact(); }
                manager->shutdown(); manager.reset(); manifest->close();
            }
        }
        for (const uint16_t version : {0, 2}) {
            auto unsupported = mf::ManifestFileHeader::build(0); unsupported.version = version; unsupported.crc32c = 0;
            if (version == 2) { unsupported.magic = 0x32564D41; }
            uint8_t bytes[mf::ManifestFileHeader::SIZE]; unsupported.serialize(bytes);
            unsupported.crc32c = akkaradb::cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes), sizeof(bytes)); unsupported.serialize(bytes);
            const auto unsupportedPath = root / ("unsupported-" + std::to_string(version) + ".akmf");
            { std::ofstream out(unsupportedPath, std::ios::binary); out.write(reinterpret_cast<const char*>(bytes), sizeof(bytes)); }
            bool rejected = false;
            try { (void)mf::Manifest::create(unsupportedPath); } catch (const std::runtime_error&) { rejected = true; }
            require(rejected, "unsupported Manifest version must be explicitly rejected");
            std::ifstream unchanged(unsupportedPath, std::ios::binary);
            const std::vector<uint8_t> actual{std::istreambuf_iterator<char>{unchanged}, std::istreambuf_iterator<char>{}};
            require(actual == std::vector<uint8_t>(bytes, bytes + sizeof(bytes)), "rejecting an unsupported version must not modify existing data");
        }
        fs::remove_all(root, ec);
    }

    std::vector<uint8_t> readFileBytes(const std::filesystem::path& path) {
        std::ifstream file(path, std::ios::binary);
        require(static_cast<bool>(file), "cannot read regression fixture");
        return {std::istreambuf_iterator<char>{file}, std::istreambuf_iterator<char>{}};
    }

    void writeManifestRotation(const std::filesystem::path& base, uint32_t sequence, uint64_t stripes) {
        namespace mf = akkaradb::engine::manifest;
        const auto path = base.parent_path() / (base.stem().string() + "-" + std::to_string(sequence) + base.extension().string());
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        uint8_t fileHeader[mf::ManifestFileHeader::SIZE];
        mf::ManifestFileHeader::build(sequence).serialize(fileHeader);
        out.write(reinterpret_cast<const char*>(fileHeader), sizeof(fileHeader));
        const auto append = [&](mf::ManifestRecordType type, const std::vector<uint8_t>& payload) {
            uint8_t header[mf::ManifestRecordHeader::SIZE];
            mf::ManifestRecordHeader::build(type, payload.data(), static_cast<uint32_t>(payload.size())).serialize(header);
            out.write(reinterpret_cast<const char*>(header), sizeof(header));
            out.write(reinterpret_cast<const char*>(payload.data()), static_cast<std::streamsize>(payload.size()));
        };
        append(mf::ManifestRecordType::CHECKPOINT, mf::encodeCheckpoint(1, "rotated", std::nullopt, 20));
        append(mf::ManifestRecordType::STRIPE_COMMIT, mf::encodeStripeCommit(1, stripes));
        out.close();
        require(static_cast<bool>(out), "cannot write rotated Manifest fixture");
    }

    int runManifestCompactCrashChild(const std::string& mode, const std::string& point, const std::filesystem::path& dir) {
        namespace mf = akkaradb::engine::manifest; namespace sst = akkaradb::engine::sst;
        const auto path = dir / (mode == "standalone" ? "sst-state.akmf" : "external.akmf");
        std::unique_ptr<mf::Manifest> manifest;
        if (mode != "standalone") { manifest = mf::Manifest::create(path, mode == "fast"); }
        sst::SSTManager::Options options; options.compactionMode = sst::SSTCompactionMode::DISABLED;
        const std::vector<uint8_t> a{'a'}, b{'b'}, value{'v'};
        auto manager = sst::SSTManager::create(dir, options, manifest.get());
        const std::vector rows{record(a, value, 10), record(b, value, 20)};
        manager->flush(rows); manager->shutdown(); manager.reset();
        if (manifest) { manifest->close(); manifest.reset(); }
        writeManifestRotation(path, 1, 1);
        writeManifestRotation(path, 2, 2);
        manifest = mf::Manifest::create(path, mode == "fast"); manifest->start();
        setSstCrashPoint(point);
        manifest->compact();
        throw std::runtime_error("Manifest child missed the requested crash boundary");
    }

    void testManifestCompactCrashRecovery(const std::filesystem::path& executable) {
        namespace fs = std::filesystem; namespace mf = akkaradb::engine::manifest; namespace sst = akkaradb::engine::sst;
        const auto root = fs::current_path() / ".bench-tmp" / "manifest_compact_crashes";
        std::error_code ec; fs::remove_all(root, ec);
        const std::vector<uint8_t> a{'a'}, b{'b'}, value{'v'};
        for (const std::string mode : {"standalone", "sync", "fast"}) {
            for (const std::string point : {"manifest.compact.after_tmp_sync", "manifest.compact.after_replace",
                "manifest.compact.after_directory_sync", "manifest.compact.after_first_rotation_delete"}) {
                const auto dir = root / (mode + "-" + point); fs::create_directories(dir);
                const auto path = dir / (mode == "standalone" ? "sst-state.akmf" : "external.akmf");
                const std::string args = "\"" + executable.string() + "\" --manifest-compact-crash-child " + mode + " " + point + " \"" + dir.string() + "\"";
#ifdef _WIN32
                const int status = std::system(("\"" + args + "\"").c_str());
                require(status == 86, "Manifest child did not stop at its crash boundary");
#else
                const int status = std::system(args.c_str());
                require(status == (86 << 8), "Manifest child did not stop at its crash boundary");
#endif
                require(fs::exists(path), "Manifest replacement left the base absent");
                for (int pass = 0; pass < 2; ++pass) {
                    std::unique_ptr<mf::Manifest> manifest;
                    if (mode != "standalone") { manifest = mf::Manifest::create(path, mode == "fast"); manifest->start(); }
                    sst::SSTManager::Options options; options.compactionMode = sst::SSTCompactionMode::DISABLED;
                    auto manager = sst::SSTManager::create(dir, options, manifest.get()); manager->recover();
                    verifySelectedVersion(*manager, a, UINT64_MAX, 10, false, value);
                    verifySelectedVersion(*manager, b, UINT64_MAX, 20, false, value);
                    manager->shutdown(); manager.reset();
                    if (manifest) { manifest->close(); manifest.reset(); }
                    manifest = mf::Manifest::create(path);
                    require(manifest->lastCheckpoint() && manifest->lastCheckpoint()->lastSeq == 20, "Manifest compaction lost the recovery checkpoint");
                    require(manifest->stripesWritten() == (pass == 0 ? 2u : 8u), "Manifest replay selected obsolete rotation state");
                    manifest->compact();
                    manifest->advance(pass == 0 ? 7u : 9u);
                    manifest->close(); manifest.reset();
                    if (pass == 0) { writeManifestRotation(path, 3, 8); }
                }
                auto final = mf::Manifest::create(path);
                require(final->stripesWritten() == 9, "appends after Manifest replacement did not survive reopen");
                final->close();
            }
        }
        fs::remove_all(root, ec);
    }

    void testManifestCorruptionPreservesSsts() {
        namespace fs = std::filesystem; namespace mf = akkaradb::engine::manifest; namespace sst = akkaradb::engine::sst;
        const auto root = fs::current_path() / ".bench-tmp" / "manifest_corruption_preserves_ssts";
        std::error_code ec; fs::remove_all(root, ec);
        const std::vector<uint8_t> value{'v'};
        const std::vector<std::vector<uint8_t>> keys{{'a'}, {'b'}, {'c'}};
        for (const std::string mode : {"standalone", "sync", "fast"}) {
            for (const bool headerCorruption : {false, true}) {
                for (const size_t recordIndex : {0u, 2u, 5u}) {
                    const auto dir = root / (mode + (headerCorruption ? "-header-" : "-payload-") + std::to_string(recordIndex));
                    fs::create_directories(dir);
                    const auto path = dir / (mode == "standalone" ? "sst-state.akmf" : "external.akmf");
                    std::unique_ptr<mf::Manifest> manifest;
                    if (mode != "standalone") { manifest = mf::Manifest::create(path, mode == "fast"); }
                    sst::SSTManager::Options options; options.compactionMode = sst::SSTCompactionMode::DISABLED;
                    auto manager = sst::SSTManager::create(dir, options, manifest.get());
                    for (size_t i = 0; i < keys.size(); ++i) {
                        const std::vector rows{record(keys[i], value, i + 1)}; manager->flush(rows);
                    }
                    manager->shutdown(); manager.reset();
                    if (manifest) { manifest->close(); manifest.reset(); }
                    std::vector<std::vector<uint8_t>> ssts;
                    for (size_t i = 1; i <= keys.size(); ++i) { ssts.push_back(readFileBytes(dir / ("L0_" + std::to_string(i) + ".aksst"))); }
                    const auto bytes = readFileBytes(path);
                    size_t offset = mf::ManifestFileHeader::SIZE;
                    for (size_t i = 0; i < recordIndex; ++i) {
                        const auto header = mf::ManifestRecordHeader::deserialize(bytes.data() + offset);
                        offset += mf::ManifestRecordHeader::SIZE + header.payloadLen;
                    }
                    flipFileByte(path, offset + (headerCorruption ? 12 : mf::ManifestRecordHeader::SIZE));
                    const auto corruptBytes = readFileBytes(path);
                    for (int pass = 0; pass < 2; ++pass) {
                        bool rejected = false;
                        try {
                            if (mode != "standalone") { manifest = mf::Manifest::create(path, mode == "fast"); }
                            manager = sst::SSTManager::create(dir, options, manifest.get()); manager->recover();
                        }
                        catch (const std::runtime_error&) { rejected = true; }
                        require(rejected, "complete corrupt Manifest record must reject recovery");
                        require(readFileBytes(path) == corruptBytes, "corruption rejection modified the Manifest");
                        for (size_t i = 1; i <= keys.size(); ++i) {
                            require(readFileBytes(dir / ("L0_" + std::to_string(i) + ".aksst")) == ssts[i - 1], "corruption rejection removed or changed a valid SST");
                        }
                    }
                }
            }
        }
        fs::remove_all(root, ec);
    }

    void testManifestIncompleteTailRepair() {
        namespace fs = std::filesystem; namespace mf = akkaradb::engine::manifest;
        const auto dir = fs::current_path() / ".bench-tmp" / "manifest_incomplete_tail";
        std::error_code ec; fs::remove_all(dir, ec); fs::create_directories(dir);
        const auto source = dir / "source.akmf";
        auto manifest = mf::Manifest::create(source);
        manifest->sstSeal(0, "retained.aksst", 1, std::nullopt, std::nullopt);
        const auto boundary = fs::file_size(source);
        manifest->checkpoint("incomplete", std::nullopt, 42);
        manifest->close(); manifest.reset();
        const auto size = fs::file_size(source);
        for (uint64_t cut = boundary + 1; cut < size; ++cut) {
            const auto path = dir / ("cut-" + std::to_string(cut) + ".akmf");
            fs::copy_file(source, path); fs::resize_file(path, cut);
            manifest = mf::Manifest::create(path);
            require(manifest->liveSst() == std::vector<std::string>{"retained.aksst"} && !manifest->lastCheckpoint(), "tail repair lost the complete prefix");
            require(fs::file_size(path) == boundary, "incomplete tail was not removed at a record boundary");
            manifest->checkpoint("repaired", std::nullopt, 43); manifest->close(); manifest.reset();
            manifest = mf::Manifest::create(path);
            require(manifest->lastCheckpoint() && manifest->lastCheckpoint()->lastSeq == 43, "appends after tail repair did not survive reopen");
            manifest->close(); manifest.reset();
        }
        const auto missing = dir / "missing.akmf";
        fs::copy_file(source, dir / "missing.akmf.compact.tmp");
        bool rejected = false;
        try { (void)mf::Manifest::create(missing); } catch (const std::runtime_error&) { rejected = true; }
        require(rejected && !fs::exists(missing), "missing base with interrupted compaction must not create an empty catalog");
        fs::remove_all(dir, ec);
    }

    void testManifestReplacementFailure() {
#ifdef _WIN32
        namespace fs = std::filesystem; namespace mf = akkaradb::engine::manifest;
        const auto dir = fs::current_path() / ".bench-tmp" / "manifest_replacement_failure";
        std::error_code ec; fs::remove_all(dir, ec); fs::create_directories(dir);
        const auto path = dir / "manifest.akmf";
        auto manifest = mf::Manifest::create(path);
        manifest->sstSeal(0, "retained.aksst", 1, std::nullopt, std::nullopt);
        const auto before = readFileBytes(path);
        HANDLE blocker = ::CreateFileW(path.c_str(), GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
        require(blocker != INVALID_HANDLE_VALUE, "cannot hold the Manifest replacement fixture open");
        bool rejected = false;
        try { manifest->compact(); } catch (const std::runtime_error&) { rejected = true; }
        ::CloseHandle(blocker);
        require(rejected && readFileBytes(path) == before, "failed replacement changed the original Manifest");
        manifest->checkpoint("after-failure", std::nullopt, 44); manifest->close(); manifest.reset();
        manifest = mf::Manifest::create(path);
        require(manifest->liveSst() == std::vector<std::string>{"retained.aksst"} && manifest->lastCheckpoint()->lastSeq == 44,
            "Manifest could not continue after a failed replacement");
        manifest->close(); manifest.reset(); fs::remove_all(dir, ec);
#endif
    }

    void testSstCorruptionDetection() {
        namespace fs = std::filesystem;
        namespace sst = akkaradb::engine::sst;

        const fs::path dir = fs::current_path() / ".bench-tmp" / "sst_corruption_detection_smoke";
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir);

        const std::vector<uint8_t> key{'k', 'e', 'y'};
        const std::vector<uint8_t> value(4096, 'v');
        const std::vector<akkaradb::core::RecordView> records{record(key, value, 1)};
        const fs::path source = dir / "source.aksst";
        (void)sst::SSTWriter::write(source, records);
        const auto header = readSstHeader(source);

        const fs::path legacy = dir / "legacy.aksst";
        fs::copy_file(source, legacy, fs::copy_options::overwrite_existing);
        removeSstMetadataChecksums(legacy);
        require(sst::SSTReader::open(legacy) == nullptr, "SST without metadata checksums must be rejected");

        const std::pair<const char*, uint64_t> metadataTargets[] = {
            {"index", header.indexOffset},
            {"key-arena", header.keyArenaOffset},
            {"bloom", header.bloomOffset},
        };
        for (const auto& [name, offset] : metadataTargets) {
            const fs::path corrupt = dir / (std::string{name} + ".aksst");
            fs::copy_file(source, corrupt, fs::copy_options::overwrite_existing);
            flipFileByte(corrupt, offset);
            require(sst::SSTReader::open(corrupt) == nullptr, "metadata corruption must reject SST open");
        }

        const fs::path corruptBlock = dir / "block.aksst";
        fs::copy_file(source, corruptBlock, fs::copy_options::overwrite_existing);
        flipFileByte(corruptBlock, header.dataOffset + sizeof(sst::SSTBlockHeaderV1));
        auto reader = sst::SSTReader::open(corruptBlock);
        require(reader != nullptr, "block corruption must be detected on block access");
        std::vector<uint8_t> out;
        bool rejected = false;
        try {
            (void)reader->getInto(bytes(key), out);
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        require(rejected, "block corruption must throw instead of reporting a miss");

        reader.reset();
        fs::remove_all(dir, ec);
    }

    #include "detail/sst_durability_regressions.inc"
}

int main(int argc, char** argv) {
    akkaradb::test::installMsvcTestErrorHandlers();

    try {
        if (argc == 3 && std::string_view{argv[1]} == "--manifest-rotation-sync-child") {
            runManifestRotationSyncChild(std::filesystem::path{argv[2]}, true);
            throw std::runtime_error("rotation child did not reach the sync boundary");
        }
        if (argc == 5 && std::string{argv[1]} == "--sst-lifecycle-crash-child") {
            return runSstLifecycleCrashChild(argv[2], argv[3], std::filesystem::path{argv[4]});
        }
        if (argc == 5 && std::string{argv[1]} == "--manifest-compact-crash-child") {
            return runManifestCompactCrashChild(argv[2], argv[3], std::filesystem::path{argv[4]});
        }
        require(argc == 1, "unexpected SST test command line");
        testVersionsWithinOneSst();
        testRecoveryRejectsInsufficientLevels();
        namespace fs = std::filesystem;
        namespace sst = akkaradb::engine::sst;

        const fs::path dir = fs::current_path() / ".bench-tmp" / "sst_snapshot_visibility_smoke";
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir);

        sst::SSTManager::Options options;
        options.compactThreads = 0;
        auto manager = sst::SSTManager::create(dir, options);

        const std::vector<uint8_t> key{'k', 'e', 'y'};
        const std::vector<uint8_t> oldValue{'o', 'l', 'd'};
        const std::vector<uint8_t> newValue{'n', 'e', 'w'};
        const std::vector<uint8_t> emptyValue;

        std::vector<akkaradb::core::RecordView> records;
        records.push_back(record(key, oldValue, 1));
        manager->flush(records);
        records.clear();

        records.push_back(record(key, newValue, 2));
        manager->flush(records);
        records.clear();

        std::vector<uint8_t> out;
        require(!manager->getInto(bytes(key), out, 0).has_value(), "snapshot 0 must not see future SST records");

        auto hit = manager->getInto(bytes(key), out, 1);
        require(hit.has_value() && *hit, "snapshot 1 must find old value");
        require(out == oldValue, "snapshot 1 returned wrong value");

        hit = manager->getInto(bytes(key), out, 2);
        require(hit.has_value() && *hit, "snapshot 2 must find new value");
        require(out == newValue, "snapshot 2 returned wrong value");

        const auto rec = manager->get(bytes(key), 1);
        require(rec.has_value() && rec->seq == 1, "snapshot get must skip newer L0 records and return older visible record");

        records.push_back(record(key, emptyValue, 3, akkaradb::core::RecordView::FLAG_TOMBSTONE));
        manager->flush(records);

        const auto beforeTombstone = manager->contains(bytes(key), 2);
        require(beforeTombstone.has_value() && *beforeTombstone, "snapshot 2 must see key before tombstone");

        const auto atTombstone = manager->contains(bytes(key), 3);
        require(atTombstone.has_value() && !*atTombstone, "snapshot 3 must see tombstone");

        hit = manager->getInto(bytes(key), out, 3);
        require(hit.has_value() && !*hit, "snapshot 3 getInto must return visible tombstone");

        auto scan = manager->scanIter({}, {}, 2);
        require(scan.hasNext(), "snapshot 2 scan must include key");
        auto scanned = scan.next();
        require(scanned.has_value() && scanned->seq == 2 && scanned->value == newValue, "snapshot 2 scan returned wrong record");
        require(!scan.hasNext(), "snapshot 2 scan must contain one live record");

        scan = manager->scanIter({}, {}, 3);
        require(!scan.hasNext(), "snapshot 3 scan must hide tombstoned key");

        manager->shutdown();
        fs::remove_all(dir, ec);
        testBlobFreeReferenceTracking();
        testManifestRecoveryPrunesCompactionArtifacts();
        testManifestCompactPreservesReplayState();
        testZstdCompressionLevel();
        testL0RecoveryUsesNumericFileOrder();
        testSstCorruptionDetection();
        testOutOfOrderSstVersions();
        testLateFlushAgainstCompactedVersions();
        testMissingSstCatalogFailsClosed();
        testSstLifecycleCrashRecovery(fs::absolute(fs::path{argv[0]}));
        testManifestV1LargeRecords();
        testLargeSstCompactionRoundTrip();
        testManifestV1LongKeysAndVersionRejection();
        testManifestCorruptionPreservesSsts();
        testManifestIncompleteTailRepair();
        testManifestReplacementFailure();
        testManifestCompactCrashRecovery(fs::absolute(fs::path{argv[0]}));
        testManifestRotationGaps();
        testManifestRotationSync(fs::absolute(fs::path{argv[0]}));
        testSstRenameFailure();
        testLiveSstBlobSnapshot();
        return 0;
    }
    catch (const std::exception& ex) {
        std::fprintf(stderr, "sst snapshot visibility smoke failed: %s\n", ex.what());
        return 1;
    }
}

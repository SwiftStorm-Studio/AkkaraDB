/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "TestErrorHandlers.hpp"

#include "akk/cpu/CRC32C.hpp"
#include "akk/core/record/KeyFingerprint.hpp"
#include "akk/core/record/RecordView.hpp"
#include "akk/engine/manifest/Manifest.hpp"
#include "akk/engine/sstable/SSTManager.hpp"
#include "akk/engine/sstable/SSTWriter.hpp"

#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

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

    [[nodiscard]] akkaradb::engine::sst::SSTFileHeaderV2 readSstHeader(const std::filesystem::path& path) {
        akkaradb::engine::sst::SSTFileHeaderV2 header{};
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
        sst::SSTFileHeaderV2 header{};
        file.read(reinterpret_cast<char*>(&header), sizeof(header));
        require(static_cast<bool>(file), "failed to read SST header for legacy-format test");

        sst::SSTFooterV2 footer{};
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
        flipFileByte(corruptBlock, header.dataOffset + sizeof(sst::SSTBlockHeaderV2));
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

}

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();

    try {
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
        testManifestRecoveryPrunesCompactionArtifacts();
        testManifestCompactPreservesReplayState();
        testZstdCompressionLevel();
        testL0RecoveryUsesNumericFileOrder();
        testSstCorruptionDetection();
        return 0;
    }
    catch (const std::exception& ex) {
        std::fprintf(stderr, "sst snapshot visibility smoke failed: %s\n", ex.what());
        return 1;
    }
}

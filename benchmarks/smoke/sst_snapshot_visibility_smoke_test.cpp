/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "TestErrorHandlers.hpp"

#include "akk/core/record/KeyFingerprint.hpp"
#include "akk/core/record/RecordView.hpp"
#include "akk/engine/manifest/Manifest.hpp"
#include "akk/engine/sstable/SSTManager.hpp"
#include "akk/engine/sstable/SSTWriter.hpp"

#include <cstdio>
#include <filesystem>
#include <span>
#include <stdexcept>
#include <string>
#include <system_error>
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
        options.sstDir = dir;
        options.compactionMode = sst::SSTCompactionMode::DISABLED;
        options.compactThreads = 0;

        const std::vector<uint8_t> alpha{'a'};
        const std::vector<uint8_t> beta{'b'};
        const std::vector<uint8_t> alphaOld{'o', 'l', 'd'};
        const std::vector<uint8_t> alphaOrphan{'o', 'r', 'p', 'h', 'a', 'n'};
        const std::vector<uint8_t> alphaNew{'n', 'e', 'w'};
        const std::vector<uint8_t> betaValue{'b', 'e', 't', 'a'};

        {
            auto manager = sst::SSTManager::create(options, mf.get());
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
            auto recovered = sst::SSTManager::create(options, mf.get());
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
            auto recovered = sst::SSTManager::create(options, mf.get());
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
        options.sstDir = dir;
        options.compactThreads = 0;
        auto manager = sst::SSTManager::create(options);

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
        return 0;
    }
    catch (const std::exception& ex) {
        std::fprintf(stderr, "sst snapshot visibility smoke failed: %s\n", ex.what());
        return 1;
    }
}

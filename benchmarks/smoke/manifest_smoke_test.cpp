/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// benchmarks/smoke/manifest_smoke_test.cpp
#include "TestErrorHandlers.hpp"

#include "akk/engine/manifest/Manifest.hpp"

#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <vector>

using namespace akkaradb::engine::manifest;

namespace {
    static std::filesystem::path makeTempDir(const std::string& suffix) {
        auto dir = std::filesystem::temp_directory_path() / ("akkaradbManifestSmoke_" + suffix);
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
        std::filesystem::create_directories(dir, ec);
        AKK_TEST_CHECK(!ec);
        return dir;
    }

    static std::filesystem::path latestManifestFile(const std::filesystem::path& base) {
        if (std::filesystem::exists(base)) {
            return base;
        }
        for (size_t i = 1; i < 10000; ++i) {
            auto rotated = base.parent_path() /
                (base.stem().string() + "-" + std::to_string(i) + base.extension().string());
            if (std::filesystem::exists(rotated)) {
                return rotated;
            }
        }
        return base;
    }

    static void testBasicStateAndReplay() {
        const auto dir = makeTempDir("basic");
        const auto path = dir / "manifest.akmf";

        {
            auto mf = Manifest::create(path, false);
            mf->start();
            mf->advance(42);
            mf->sstSeal(0, "L0_0001.aksst", 100, std::optional<std::string>{"aa"}, std::optional<std::string>{"ff"});
            mf->sstDelete("L0_0001.aksst");
            mf->sstSeal(0, "L0_0002.aksst", 200, std::nullopt, std::nullopt);
            mf->checkpoint(std::optional<std::string>{"cp1"}, std::optional<uint64_t>{42}, std::optional<uint64_t>{1000});
            mf->close();
        }

        {
            auto mf = Manifest::create(path, false);
            const auto live = mf->liveSst();
            AKK_TEST_CHECK(live.size() == 1);
            AKK_TEST_CHECK(live[0] == "L0_0002.aksst");

            const auto deleted = mf->deletedSst();
            AKK_TEST_CHECK(deleted.empty());

            const auto cp = mf->lastCheckpoint();
            AKK_TEST_CHECK(cp.has_value());
            AKK_TEST_CHECK(cp->name.has_value() && cp->name.value() == "cp1");
            AKK_TEST_CHECK(cp->stripe.has_value() && cp->stripe.value() == 42);
            AKK_TEST_CHECK(cp->lastSeq.has_value() && cp->lastSeq.value() == 1000);
            AKK_TEST_CHECK(mf->stripesWritten() == 42);
        }
    }

    static void testCompactionCommit() {
        const auto dir = makeTempDir("commit");
        const auto path = dir / "manifest.akmf";

        auto mf = Manifest::create(path, false);
        mf->sstSeal(0, "in1.aksst", 10, std::nullopt, std::nullopt);
        mf->sstSeal(0, "in2.aksst", 20, std::nullopt, std::nullopt);
        mf->compactionCommit({"out1.aksst", "out2.aksst"}, {"in1.aksst", "in2.aksst"});

        const auto live = mf->liveSst();
        bool hasOut1 = false;
        bool hasOut2 = false;
        bool hasIn1 = false;
        for (const auto& f : live) {
            if (f == "out1.aksst") hasOut1 = true;
            if (f == "out2.aksst") hasOut2 = true;
            if (f == "in1.aksst") hasIn1 = true;
        }
        AKK_TEST_CHECK(hasOut1 && hasOut2);
        AKK_TEST_CHECK(!hasIn1);
    }

    static void testCrcStopOnCorruption() {
        const auto dir = makeTempDir("crc");
        const auto path = dir / "manifest.akmf";

        {
            auto mf = Manifest::create(path, false);
            mf->sstSeal(0, "a.aksst", 1, std::nullopt, std::nullopt);
            mf->sstSeal(0, "b.aksst", 1, std::nullopt, std::nullopt);
            mf->close();
        }

        {
            std::fstream file(latestManifestFile(path), std::ios::in | std::ios::out | std::ios::binary);
            AKK_TEST_CHECK(file.good());
            // Corrupt first record payload byte after 32B file header + 8B rec header.
            file.seekp(40, std::ios::beg);
            char bad = '\x7f';
            file.write(&bad, 1);
        }

        {
            auto mf = Manifest::create(path, false);
            // replay stops at first CRC mismatch; no valid SST state should be rebuilt.
            AKK_TEST_CHECK(mf->liveSst().empty());
        }
    }

    static void testClusterEventsInProcessState() {
        const auto dir = makeTempDir("cluster");
        const auto path = dir / "manifest.akmf";

        auto mf = Manifest::create(path, false);
        mf->nodeJoin(7, 20481, "127.0.0.1");
        mf->primaryLease(7, 123456789);
        mf->nodeLeave(7);

        const auto joins = mf->nodeJoins();
        const auto leaves = mf->nodeLeaves();
        const auto lease = mf->lastPrimaryLease();
        AKK_TEST_CHECK(joins.size() == 1);
        AKK_TEST_CHECK(joins.front().nodeId == 7);
        AKK_TEST_CHECK(joins.front().replPort == 20481);
        AKK_TEST_CHECK(joins.front().host == "127.0.0.1");
        AKK_TEST_CHECK(leaves.size() == 1);
        AKK_TEST_CHECK(leaves.front().nodeId == 7);
        AKK_TEST_CHECK(lease.has_value());
        AKK_TEST_CHECK(lease->nodeId == 7);
        AKK_TEST_CHECK(lease->leaseUntilUs == 123456789);
    }

    static void testRotationNamingUsesDashBeforeExtension() {
        const auto dir = makeTempDir("rotation");
        const auto path = dir / "manifest.akmf";

        auto mf = Manifest::create(path, false);
        mf->start();

        std::string hostChunk(60 * 1024, 'x');
        bool rotated = false;
        for (int i = 0; i < 1024; ++i) {
            mf->nodeJoin(static_cast<uint64_t>(i + 1), static_cast<uint16_t>(20001 + (i % 8)), hostChunk);
            if (std::filesystem::exists(dir / "manifest-1.akmf")) {
                rotated = true;
                break;
            }
        }
        mf->nodeLeave(1);
        mf->close();

        AKK_TEST_CHECK(std::filesystem::exists(path));
        AKK_TEST_CHECK(rotated);
        AKK_TEST_CHECK(std::filesystem::exists(dir / "manifest-1.akmf"));
        AKK_TEST_CHECK(!std::filesystem::exists(dir / "manifest.akmf.1"));
    }
} // namespace

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();

    testBasicStateAndReplay();
    testCompactionCommit();
    testCrcStopOnCorruption();
    testClusterEventsInProcessState();
    testRotationNamingUsesDashBeforeExtension();
    std::printf("manifest smoke test passed\n");
    return 0;
}
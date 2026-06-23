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

// benchmarks/smoke/versionlogSmokeTest.cpp
#include "TestErrorHandlers.hpp"

#include "akk/engine/vlog/VersionLog.hpp"

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <vector>

using namespace akkaradb::engine::vlog;

namespace {
    static std::span<const uint8_t> asU8(std::string_view sv) {
        return {reinterpret_cast<const uint8_t*>(sv.data()), sv.size()};
    }

    static std::string to_string(std::span<const uint8_t> value) {
        return {reinterpret_cast<const char*>(value.data()), value.size()};
    }

    static std::filesystem::path makeTempDir(const std::string& suffix) {
        auto dir = std::filesystem::temp_directory_path() / ("akkaradbVlogSmoke_" + suffix);
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
        std::filesystem::create_directories(dir, ec);
        AKK_TEST_CHECK(!ec);
        return dir;
    }

    static void testAppendAndQueries() {
        const auto dir = makeTempDir("appendQueries");
        const auto path = dir / "history.akvlog";

        auto log = VersionLog::create(VersionLogOptions{.logPath = path, .syncMode = VLogSyncMode::ASYNC});
        log->append(asU8("k"), 10, 7, 1000, 0x00, asU8("v1"));
        log->append(asU8("k"), 20, 7, 2000, 0x00, asU8("v2"));
        log->append(asU8("k"), 30, 7, 3000, 0x01, {});

        {
            const auto h = log->history(asU8("k"));
            AKK_TEST_CHECK(h.size() == 3);
            AKK_TEST_CHECK(h[0].seq == 10);
            AKK_TEST_CHECK(h[1].seq == 20);
            AKK_TEST_CHECK(h[2].seq == 30);
        }

        {
            const auto snap = log->snapshot();
            AKK_TEST_CHECK(snap.syncMode == static_cast<uint8_t>(VLogSyncMode::ASYNC));
            AKK_TEST_CHECK(snap.indexedKeys == 1);
            AKK_TEST_CHECK(snap.indexedEntries == 3);
            AKK_TEST_CHECK(snap.rollbackEntries == 0);
            AKK_TEST_CHECK(snap.flushThreadRunning);
        }

        {
            const auto v = log->getAt(asU8("k"), 5);
            AKK_TEST_CHECK(!v.has_value());
        }
        {
            const auto v = log->getAt(asU8("k"), 20);
            AKK_TEST_CHECK(v.has_value());
            AKK_TEST_CHECK(to_string(v->value) == "v2");
        }
        {
            const auto v = log->getAt(asU8("k"), 999);
            AKK_TEST_CHECK(v.has_value());
            AKK_TEST_CHECK(v->flags == 0x01);
        }
    }

    static void testRecoveryAndCorruptionDetection() {
        const auto dir = makeTempDir("recovery");
        const auto path = dir / "history.akvlog";

        {
            auto log = VersionLog::create(VersionLogOptions{.logPath = path, .syncMode = VLogSyncMode::SYNC});
            log->append(asU8("a"), 1, 1, 100, 0x00, asU8("x"));
            log->append(asU8("a"), 2, 1, 200, 0x00, asU8("y"));
            log->append(asU8("b"), 3, 1, 300, 0x00, asU8("z"));
            log->close();
        }

        {
            auto log = VersionLog::create(VersionLogOptions{.logPath = path, .syncMode = VLogSyncMode::ASYNC});
            const auto va = log->getAt(asU8("a"), 999);
            AKK_TEST_CHECK(va.has_value());
            AKK_TEST_CHECK(to_string(va->value) == "y");
        }

        {
            std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
            AKK_TEST_CHECK(file.good());
            file.seekp(40, std::ios::beg);
            char bad = '\x7f';
            file.write(&bad, 1);
        }

        {
            bool threw = false;
            try {
                auto log = VersionLog::create(VersionLogOptions{.logPath = path, .syncMode = VLogSyncMode::ASYNC});
                (void)log;
            }
            catch (const std::runtime_error&) {
                threw = true;
            }
            AKK_TEST_CHECK(threw);
        }
    }

    static void testCollectRollbackTargets() {
        const auto dir = makeTempDir("rollbackTargets");
        const auto path = dir / "history.akvlog";

        auto log = VersionLog::create(VersionLogOptions{.logPath = path, .syncMode = VLogSyncMode::ASYNC});
        log->append(asU8("k1"), 10, 1, 10, 0x00, asU8("v1"));
        log->append(asU8("k1"), 40, ROLLBACK_NODE, 40, static_cast<uint8_t>(0x00 | VLOG_FLAG_ROLLBACK), asU8("v2"));
        log->append(asU8("k2"), 20, 1, 20, 0x00, asU8("x1"));

        const auto targets = log->collectRollbackTargets(25);
        AKK_TEST_CHECK(targets.size() == 1);
        AKK_TEST_CHECK(std::string(targets[0].first.begin(), targets[0].first.end()) == "k1");
        AKK_TEST_CHECK(targets[0].second.has_value());
        AKK_TEST_CHECK(targets[0].second->seq == 10);

        const auto snap = log->snapshot();
        AKK_TEST_CHECK(snap.indexedKeys == 2);
        AKK_TEST_CHECK(snap.indexedEntries == 3);
        AKK_TEST_CHECK(snap.rollbackEntries == 1);
    }

    static void testBatchedSyncRecovery() {
        const auto dir = makeTempDir("batchedSync");
        const auto path = dir / "history.akvlog";

        {
            auto log = VersionLog::create(
                VersionLogOptions{
                    .logPath = path,
                    .syncMode = VLogSyncMode::BATCHED_SYNC,
                    .groupN = 4,
                    .groupMicros = 0,
                    .groupBytes = 256,
                }
            );
            log->append(asU8("k"), 1, 11, 100, 0x00, asU8("v1"));
            log->append(asU8("k"), 2, 11, 200, 0x00, asU8("v2"));
            log->close();
        }

        {
            auto log = VersionLog::create(VersionLogOptions{.logPath = path, .syncMode = VLogSyncMode::ASYNC});
            const auto v = log->getAt(asU8("k"), 2);
            AKK_TEST_CHECK(v.has_value());
            AKK_TEST_CHECK(to_string(v->value) == "v2");
        }
    }
} // namespace

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();

    testAppendAndQueries();
    testRecoveryAndCorruptionDetection();
    testCollectRollbackTargets();
    testBatchedSyncRecovery();
    std::printf("versionlog smoke test passed\n");
    return 0;
}

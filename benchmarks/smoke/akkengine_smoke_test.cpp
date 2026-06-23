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

// benchmarks/smoke/akkengineSmokeTest.cpp
#include "TestErrorHandlers.hpp"

#include "akk/engine/AkkEngine.hpp"
#include "akk/engine/vlog/VersionLog.hpp"

#include <filesystem>
#include <stdexcept>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace fs = std::filesystem;
using namespace akkaradb::engine;

namespace {
    [[nodiscard]] std::span<const uint8_t> bytes(std::string_view value) {
        return {reinterpret_cast<const uint8_t*>(value.data()), value.size()};
    }

    [[nodiscard]] std::string text(const std::vector<uint8_t>& value) {
        return {reinterpret_cast<const char*>(value.data()), value.size()};
    }

    [[nodiscard]] std::string text(std::span<const uint8_t> value) {
        return {reinterpret_cast<const char*>(value.data()), value.size()};
    }

    [[nodiscard]] fs::path tempDir(std::string_view name) {
        auto path = fs::temp_directory_path() / "akkaradbAkkengineSmoke" / std::string{name};
        fs::remove_all(path);
        fs::create_directories(path);
        return path;
    }

    void testMemoryBasic() {
        AkkEngineOptions opts;
        opts.components.walEnabled = false;
        opts.components.blobEnabled = false;
        opts.components.manifestEnabled = false;
        opts.components.sstEnabled = false;

        auto engine = AkkEngine::open(opts);
        engine->put(bytes("k"), bytes("v1"));
        AKK_TEST_CHECK(text(*engine->get(bytes("k"))) == "v1");
        AKK_TEST_CHECK(engine->exists(bytes("k")));

        engine->put(bytes("k"), bytes("v2"));
        std::vector<uint8_t> out;
        AKK_TEST_CHECK(engine->getInto(bytes("k"), out));
        AKK_TEST_CHECK(text(out) == "v2");

        engine->remove(bytes("k"));
        AKK_TEST_CHECK(!engine->get(bytes("k")));
        AKK_TEST_CHECK(!engine->exists(bytes("k")));

        engine->put(bytes("a"), bytes("1"));
        engine->put(bytes("b"), bytes("2"));
        AKK_TEST_CHECK(engine->count(bytes("a"), bytes("c")) == 2);

        const auto stats = engine->stats();
        AKK_TEST_CHECK(stats.putsTotal >= 4);
        AKK_TEST_CHECK(stats.removesTotal >= 1);
        AKK_TEST_CHECK(stats.getsTotal >= 3);
        AKK_TEST_CHECK(stats.existsTotal >= 2);
        AKK_TEST_CHECK(stats.memtable.shardCount > 0);

        engine->close();
        engine->close();
    }

    void testWalRecovery() {
        const auto dir = tempDir("wal");

        {
            AkkEngineOptions opts;
            opts.paths.dataDir = dir;
            opts.components.blobEnabled = false;
            opts.components.manifestEnabled = false;
            opts.components.sstEnabled = false;
            auto engine = AkkEngine::open(opts);
            engine->put(bytes("a"), bytes("1"));
            engine->forceSync();
        }

        {
            AkkEngineOptions opts;
            opts.paths.dataDir = dir;
            opts.components.blobEnabled = false;
            opts.components.manifestEnabled = false;
            opts.components.sstEnabled = false;
            auto engine = AkkEngine::open(opts);
            AKK_TEST_CHECK(text(*engine->get(bytes("a"))) == "1");
        }
    }

    void testBlob() {
        const auto dir = tempDir("blob");
        AkkEngineOptions opts;
        opts.paths.dataDir = dir;
        opts.components.manifestEnabled = false;
        opts.components.sstEnabled = false;
        opts.blob.thresholdBytes = 4;

        auto engine = AkkEngine::open(opts);
        engine->put(bytes("blob"), bytes("large-value"));
        AKK_TEST_CHECK(text(*engine->get(bytes("blob"))) == "large-value");
    }

    void testBlobGc() {
        const auto dir = tempDir("blob_gc");
        AkkEngineOptions opts;
        opts.paths.dataDir = dir;
        opts.components.manifestEnabled = false;
        opts.components.sstEnabled = false;
        opts.blob.thresholdBytes = 4;
        opts.blob.gcOnClose = true;

        {
            auto engine = AkkEngine::open(opts);
            engine->put(bytes("blob"), bytes("large-value"));
            const auto oldBlobPath = dir / "blobs" / "00" / "0000000000000001.akblob";
            AKK_TEST_CHECK(fs::exists(oldBlobPath));
            engine->put(bytes("blob"), bytes("x"));
            AKK_TEST_CHECK(text(*engine->get(bytes("blob"))) == "x");
            engine->runBlobGc();
            engine->close();
            AKK_TEST_CHECK(!fs::exists(oldBlobPath));
        }
    }

    void testFlushAndScan() {
        const auto dir = tempDir("flush");
        AkkEngineOptions opts;
        opts.paths.dataDir = dir;
        opts.components.blobEnabled = false;
        opts.memtable.thresholdBytesPerShard = 1;

        auto engine = AkkEngine::open(opts);
        engine->put(bytes("a"), bytes("1"));
        engine->put(bytes("b"), bytes("2"));
        engine->remove(bytes("a"));
        engine->forceFlush();

        AKK_TEST_CHECK(!engine->get(bytes("a")));
        AKK_TEST_CHECK(text(*engine->get(bytes("b"))) == "2");

        akkaradb::core::BufferArena scanArena;
        auto rows = engine->scan(scanArena);
        auto it = rows.begin();
        int count = 0;
        while (!(it == rows.end())) {
            const auto& item = *it;
            AKK_TEST_CHECK(text(item.key) == "b");
            AKK_TEST_CHECK(text(item.value) == "2");
            ++count;
            ++it;
        }
        AKK_TEST_CHECK(count == 1);
    }

    void testVersionLog() {
        const auto dir = tempDir("vlog");
        AkkEngineOptions opts;
        opts.paths.dataDir = dir;
        opts.components.blobEnabled = false;
        opts.components.manifestEnabled = false;
        opts.components.sstEnabled = false;
        opts.components.versionLogEnabled = true;

        auto engine = AkkEngine::open(opts);
        engine->put(bytes("v"), bytes("one"));
        engine->put(bytes("v"), bytes("two"));
        const auto hist = engine->history(bytes("v"));
        AKK_TEST_CHECK(hist.size() == 2);
        AKK_TEST_CHECK(text(*engine->getAt(bytes("v"), hist[0].seq)) == "one");
        engine->rollbackKey(bytes("v"), hist[0].seq);
        AKK_TEST_CHECK(text(*engine->get(bytes("v"))) == "one");
        const auto afterRollback = engine->history(bytes("v"));
        AKK_TEST_CHECK(afterRollback.size() == 3);
        AKK_TEST_CHECK(afterRollback.back().sourceNodeId == akkaradb::engine::vlog::ROLLBACK_NODE);
        AKK_TEST_CHECK((afterRollback.back().flags & akkaradb::engine::vlog::VLOG_FLAG_ROLLBACK) != 0);

        const auto stats = engine->stats();
        AKK_TEST_CHECK(stats.vlog.enabled);
        AKK_TEST_CHECK(stats.vlog.syncMode == static_cast<uint32_t>(akkaradb::engine::vlog::VLogSyncMode::ASYNC));
        AKK_TEST_CHECK(stats.vlog.indexedKeys == 1);
        AKK_TEST_CHECK(stats.vlog.indexedEntries == 3);
        AKK_TEST_CHECK(stats.vlog.rollbackEntries == 1);
        AKK_TEST_CHECK(stats.vlog.asyncMaxPendingBytes == opts.vlog.asyncMaxPendingBytes);
        AKK_TEST_CHECK(stats.vlog.flushThreadRunning);
        AKK_TEST_CHECK(stats.vlog.durableBytes > 0);
    }

    void testBlobGcRejectedWithVersionLog() {
        const auto dir = tempDir("blob_gc_vlog");
        AkkEngineOptions opts;
        opts.paths.dataDir = dir;
        opts.components.manifestEnabled = false;
        opts.components.sstEnabled = false;
        opts.components.versionLogEnabled = true;
        opts.blob.thresholdBytes = 4;

        auto engine = AkkEngine::open(opts);
        engine->put(bytes("blob"), bytes("large-value"));
        bool threw = false;
        try {
            engine->runBlobGc();
        }
        catch (const std::runtime_error&) {
            threw = true;
        }
        AKK_TEST_CHECK(threw);
    }
}

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();

    testMemoryBasic();
    testWalRecovery();
    testBlob();
    testBlobGc();
    testFlushAndScan();
    testVersionLog();
    testBlobGcRejectedWithVersionLog();
    return 0;
}

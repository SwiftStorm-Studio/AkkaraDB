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

// benchmarks/smoke/sstableSmokeTest.cpp
#include "TestErrorHandlers.hpp"

#include "akk/core/record/KeyFingerprint.hpp"
#include "akk/core/record/SSTHdr32.hpp"
#include "akk/engine/sstable/SSTManager.hpp"
#include "akk/engine/sstable/SSTReader.hpp"
#include "akk/engine/sstable/SSTWriter.hpp"
#include "akk/engine/memtable/MemTable.hpp"
#include "akk/engine/manifest/Manifest.hpp"

#include <chrono>
#include <filesystem>
#include <format>
#include <iostream>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;
using namespace akkaradb;
namespace sst = akkaradb::engine::sst;

namespace {
    std::vector<uint8_t> bytes(std::string_view s) {
        return {reinterpret_cast<const uint8_t*>(s.data()), reinterpret_cast<const uint8_t*>(s.data() + s.size())};
    }

    std::string str(std::span<const uint8_t> s) {
        return {reinterpret_cast<const char*>(s.data()), s.size()};
    }

    fs::path tempDir(std::string_view name) {
        auto p = fs::temp_directory_path() / std::format("akkara_{}_{}", name, std::chrono::steady_clock::now().time_since_epoch().count());
        fs::remove_all(p);
        fs::create_directories(p);
        return p;
    }

    struct Records {
        std::vector<std::vector<uint8_t>> keys;
        std::vector<std::vector<uint8_t>> vals;
        std::vector<core::RecordView> views;
    };

    Records makeRecords(int begin, int count, uint64_t seqBase = 1) {
        Records r;
        r.keys.reserve(static_cast<size_t>(count));
        r.vals.reserve(static_cast<size_t>(count));
        r.views.reserve(static_cast<size_t>(count));
        for (int i = 0; i < count; ++i) {
            r.keys.push_back(bytes(std::format("key_{:04}", begin + i)));
            r.vals.push_back(bytes(std::format("value_{:04}", begin + i)));
        }
        for (int i = 0; i < count; ++i) {
            const auto& k = r.keys[static_cast<size_t>(i)];
            const auto& v = r.vals[static_cast<size_t>(i)];
            const uint64_t fp = core::computeKeyFp64(k.data(), k.size());
            const uint64_t mk = core::buildMiniKey(k.data(), k.size());
            r.views.emplace_back(k.data(), static_cast<uint16_t>(k.size()), v.data(), static_cast<uint16_t>(v.size()), seqBase + static_cast<uint64_t>(i), core::SSTHdr32::FLAG_NORMAL, fp, mk);
        }
        return r;
    }

    void testWriterReaderRoundtrip() {
        auto dir = tempDir("sstRoundtrip");
        auto recs = makeRecords(0, 256);
        const auto path = dir / "one.aksst";

        sst::SSTWriter::Options opts;
        opts.blockSize = 4096;
        opts.codec = sst::SSTWriter::Codec::ZSTD;
        const auto result = sst::SSTWriter::write(path, recs.views, opts);
        AKK_TEST_CHECK(result.entryCount == 256);

        auto reader = sst::SSTReader::open(path);
        AKK_TEST_CHECK(reader);
        AKK_TEST_CHECK(reader->header().magic == sst::SST_MAGIC_V2);
        AKK_TEST_CHECK(reader->header().version == sst::SST_VERSION_V2);

        auto found = reader->get(bytes("key_0042"));
        AKK_TEST_CHECK(found);
        AKK_TEST_CHECK(str(found->value) == "value_0042");
        AKK_TEST_CHECK(!reader->contains(bytes("missing")).has_value());

        std::vector<uint8_t> out;
        auto got = reader->getInto(bytes("key_0100"), out);
        AKK_TEST_CHECK(got.has_value() && *got);
        AKK_TEST_CHECK(str(out) == "value_0100");

        size_t scanned = 0;
        auto scan = reader->scan(bytes("key_0010"), bytes("key_0020"));
        for (auto&& rec : scan) {
            AKK_TEST_CHECK(str(rec.key) >= "key_0010");
            AKK_TEST_CHECK(str(rec.key) < "key_0020");
            ++scanned;
        }
        AKK_TEST_CHECK(scanned == 10);
        reader.reset();
        fs::remove_all(dir);
    }

    void testMemtableFlushManagerRecover() {
        auto dir = tempDir("sstManager");
        auto manifest = engine::manifest::Manifest::create(dir / "manifest.akmf", false);

        sst::SSTManager::Options sopts;
        sopts.sstDir = dir / "sst";
        sopts.maxL0Files = 8;
        sopts.blockSize = 4096;
        auto manager = sst::SSTManager::create(sopts, manifest.get());
        manager->recover();

        engine::memtable::MemTable::Options mopts;
        mopts.shardCount = 1;
        mopts.thresholdBytesPerShard = 1ULL << 30;
        mopts.onFlush = [&](std::span<const core::RecordView> batch) { manager->flush(batch); };
        auto mem = engine::memtable::MemTable::create(mopts);
        for (int i = 0; i < 64; ++i) {
            auto k = bytes(std::format("key_{:04}", i));
            auto v = bytes(std::format("value_{:04}", i));
            mem->put(k, v, mem->nextSeq());
        }
        mem->forceFlush();
        manager->shutdown();
        manager.reset();
        manifest->close();
        manifest.reset();

        auto manifest2 = engine::manifest::Manifest::create(dir / "manifest.akmf", false);
        auto manager2 = sst::SSTManager::create(sopts, manifest2.get());
        manager2->recover();
        auto rec = manager2->get(bytes("key_0020"));
        AKK_TEST_CHECK(rec);
        AKK_TEST_CHECK(str(rec->value) == "value_0020");
        manager2->shutdown();
        manager2.reset();
        manifest2->close();
        manifest2.reset();
        fs::remove_all(dir);
    }

    void testCompactionOverwriteAndTombstone() {
        auto dir = tempDir("sstCompact");
        auto manifest = engine::manifest::Manifest::create(dir / "manifest.akmf", false);

        sst::SSTManager::Options sopts;
        sopts.sstDir = dir / "sst";
        sopts.maxLevels = 2;
        sopts.maxL0Files = 2;
        sopts.blockSize = 4096;
        auto manager = sst::SSTManager::create(sopts, manifest.get());
        manager->recover();

        auto v1Key = bytes("dup");
        auto v1Val = bytes("v1");
        const uint64_t fp = core::computeKeyFp64(v1Key.data(), v1Key.size());
        const uint64_t mk = core::buildMiniKey(v1Key.data(), v1Key.size());
        std::vector<core::RecordView> batch1;
        batch1.emplace_back(v1Key.data(), static_cast<uint16_t>(v1Key.size()), v1Val.data(), static_cast<uint16_t>(v1Val.size()), 1, core::SSTHdr32::FLAG_NORMAL, fp, mk);
        manager->flush(batch1);

        std::vector<uint8_t> empty;
        std::vector<core::RecordView> batch2;
        batch2.emplace_back(v1Key.data(), static_cast<uint16_t>(v1Key.size()), empty.data(), static_cast<uint16_t>(0), 2, core::SSTHdr32::FLAG_TOMBSTONE, fp, mk);
        manager->flush(batch2);

        for (int i = 0; i < 100; ++i) {
            if (manager->compactionSnapshot().compactionsCompleted > 0) { break; }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        auto c = manager->contains(v1Key);
        AKK_TEST_CHECK(!c.has_value());
        manager->shutdown();
        manager.reset();
        manifest->close();
        manifest.reset();
        fs::remove_all(dir);
    }

    void testManagerScanIterMergesLazily() {
        auto dir = tempDir("sstScanIter");
        auto manifest = engine::manifest::Manifest::create(dir / "manifest.akmf", false);

        sst::SSTManager::Options sopts;
        sopts.sstDir = dir / "sst";
        sopts.maxL0Files = 100;
        sopts.blockSize = 4096;
        auto manager = sst::SSTManager::create(sopts, manifest.get());
        manager->recover();

        auto makeView = [](std::vector<uint8_t>& key, std::vector<uint8_t>& value, uint64_t seq, uint8_t flags) {
            const uint64_t fp = core::computeKeyFp64(key.data(), key.size());
            const uint64_t mk = core::buildMiniKey(key.data(), key.size());
            return core::RecordView(key.data(), static_cast<uint16_t>(key.size()), value.data(), static_cast<uint16_t>(value.size()), seq, flags, fp, mk);
        };

        auto a = bytes("a");
        auto b = bytes("b");
        auto c = bytes("c");
        auto d = bytes("d");
        auto va = bytes("old-a");
        auto vb1 = bytes("old-b");
        auto vc1 = bytes("old-c");
        std::vector<core::RecordView> batch1;
        batch1.push_back(makeView(a, va, 1, core::SSTHdr32::FLAG_NORMAL));
        batch1.push_back(makeView(b, vb1, 2, core::SSTHdr32::FLAG_NORMAL));
        batch1.push_back(makeView(c, vc1, 3, core::SSTHdr32::FLAG_NORMAL));
        manager->flush(batch1);

        auto vb2 = bytes("new-b");
        auto empty = bytes("");
        auto vd = bytes("new-d");
        std::vector<core::RecordView> batch2;
        batch2.push_back(makeView(b, vb2, 10, core::SSTHdr32::FLAG_NORMAL));
        batch2.push_back(makeView(c, empty, 11, core::SSTHdr32::FLAG_TOMBSTONE));
        batch2.push_back(makeView(d, vd, 12, core::SSTHdr32::FLAG_NORMAL));
        manager->flush(batch2);

        std::vector<std::string> keys;
        std::vector<std::string> values;
        {
            auto it = manager->scanIter();
            while (it.hasNext()) {
                auto rec = it.next();
                if (!rec) { throw std::runtime_error("scanIter hasNext returned true but next returned nullopt"); }
                keys.push_back(str(rec->key));
                values.push_back(str(rec->value));
            }
        }
        if (keys != std::vector<std::string>{"a", "b", "d"} || values != std::vector<std::string>{"old-a", "new-b", "new-d"}) {
            std::string msg = "scanIter mismatch\nkeys:";
            for (const auto& key : keys) { msg += " " + key; }
            msg += "\nvalues:";
            for (const auto& value : values) { msg += " " + value; }
            throw std::runtime_error(msg);
        }

        manager->shutdown();
        manager.reset();
        manifest->close();
        manifest.reset();
        fs::remove_all(dir);
    }
}

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();

    testWriterReaderRoundtrip();
    testMemtableFlushManagerRecover();
    testCompactionOverwriteAndTombstone();
    testManagerScanIterMergesLazily();
    std::cout << "SST v2 smoke tests passed\n";
    return 0;
}

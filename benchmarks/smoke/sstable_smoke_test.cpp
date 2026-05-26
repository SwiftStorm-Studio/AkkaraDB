/*
 * AkkaraDB - SST v2 smoke tests
 */

#include "akk/core/record/KeyFingerprint.hpp"
#include "akk/core/record/SSTHdr32.hpp"
#include "akk/engine/sstable/SSTManager.hpp"
#include "akk/engine/sstable/SSTReader.hpp"
#include "akk/engine/sstable/SSTWriter.hpp"
#include "akk/engine/memtable/MemTable.hpp"
#include "akk/engine/manifest/Manifest.hpp"

#include <cassert>
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

    fs::path temp_dir(std::string_view name) {
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

    Records make_records(int begin, int count, uint64_t seq_base = 1) {
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
            const uint64_t fp = core::compute_key_fp64(k.data(), k.size());
            const uint64_t mk = core::build_mini_key(k.data(), k.size());
            r.views.emplace_back(k.data(), static_cast<uint16_t>(k.size()), v.data(), static_cast<uint16_t>(v.size()), seq_base + static_cast<uint64_t>(i), core::SSTHdr32::FLAG_NORMAL, fp, mk);
        }
        return r;
    }

    void test_writer_reader_roundtrip() {
        auto dir = temp_dir("sst_roundtrip");
        auto recs = make_records(0, 256);
        const auto path = dir / "one.aksst";

        sst::SSTWriter::Options opts;
        opts.block_size = 4096;
        opts.codec = sst::SSTWriter::Codec::Zstd;
        const auto result = sst::SSTWriter::write(path, recs.views, opts);
        assert(result.entry_count == 256);

        auto reader = sst::SSTReader::open(path);
        assert(reader);
        assert(reader->header().magic == sst::SST_MAGIC_V2);
        assert(reader->header().version == sst::SST_VERSION_V2);

        auto found = reader->get(bytes("key_0042"));
        assert(found);
        assert(str(found->value) == "value_0042");
        assert(!reader->contains(bytes("missing")).has_value());

        std::vector<uint8_t> out;
        auto got = reader->get_into(bytes("key_0100"), out);
        assert(got.has_value() && *got);
        assert(str(out) == "value_0100");

        size_t scanned = 0;
        auto scan = reader->scan(bytes("key_0010"), bytes("key_0020"));
        for (auto&& rec : scan) {
            assert(str(rec.key) >= "key_0010");
            assert(str(rec.key) < "key_0020");
            ++scanned;
        }
        assert(scanned == 10);
        reader.reset();
        fs::remove_all(dir);
    }

    void test_memtable_flush_manager_recover() {
        auto dir = temp_dir("sst_manager");
        auto manifest = engine::manifest::Manifest::create(dir / "manifest.akmf", false);

        sst::SSTManager::Options sopts;
        sopts.sst_dir = dir / "sst";
        sopts.max_l0_files = 8;
        sopts.block_size = 4096;
        auto manager = sst::SSTManager::create(sopts, manifest.get());
        manager->recover();

        engine::memtable::MemTable::Options mopts;
        mopts.shard_count = 1;
        mopts.threshold_bytes_per_shard = 1ULL << 30;
        mopts.on_flush = [&](std::span<const core::RecordView> batch) { manager->flush(batch); };
        auto mem = engine::memtable::MemTable::create(mopts);
        for (int i = 0; i < 64; ++i) {
            auto k = bytes(std::format("key_{:04}", i));
            auto v = bytes(std::format("value_{:04}", i));
            mem->put(k, v, mem->next_seq());
        }
        mem->force_flush();
        manager->shutdown();
        manager.reset();
        manifest->close();
        manifest.reset();

        auto manifest2 = engine::manifest::Manifest::create(dir / "manifest.akmf", false);
        auto manager2 = sst::SSTManager::create(sopts, manifest2.get());
        manager2->recover();
        auto rec = manager2->get(bytes("key_0020"));
        assert(rec);
        assert(str(rec->value) == "value_0020");
        manager2->shutdown();
        manager2.reset();
        manifest2->close();
        manifest2.reset();
        fs::remove_all(dir);
    }

    void test_compaction_overwrite_and_tombstone() {
        auto dir = temp_dir("sst_compact");
        auto manifest = engine::manifest::Manifest::create(dir / "manifest.akmf", false);

        sst::SSTManager::Options sopts;
        sopts.sst_dir = dir / "sst";
        sopts.max_levels = 2;
        sopts.max_l0_files = 2;
        sopts.block_size = 4096;
        auto manager = sst::SSTManager::create(sopts, manifest.get());
        manager->recover();

        auto v1_key = bytes("dup");
        auto v1_val = bytes("v1");
        const uint64_t fp = core::compute_key_fp64(v1_key.data(), v1_key.size());
        const uint64_t mk = core::build_mini_key(v1_key.data(), v1_key.size());
        std::vector<core::RecordView> batch1;
        batch1.emplace_back(v1_key.data(), static_cast<uint16_t>(v1_key.size()), v1_val.data(), static_cast<uint16_t>(v1_val.size()), 1, core::SSTHdr32::FLAG_NORMAL, fp, mk);
        manager->flush(batch1);

        std::vector<uint8_t> empty;
        std::vector<core::RecordView> batch2;
        batch2.emplace_back(v1_key.data(), static_cast<uint16_t>(v1_key.size()), empty.data(), static_cast<uint16_t>(0), 2, core::SSTHdr32::FLAG_TOMBSTONE, fp, mk);
        manager->flush(batch2);

        for (int i = 0; i < 100; ++i) {
            if (manager->compaction_snapshot().compactions_completed > 0) { break; }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        auto c = manager->contains(v1_key);
        assert(!c.has_value());
        manager->shutdown();
        manager.reset();
        manifest->close();
        manifest.reset();
        fs::remove_all(dir);
    }

    void test_manager_scan_iter_merges_lazily() {
        auto dir = temp_dir("sst_scan_iter");
        auto manifest = engine::manifest::Manifest::create(dir / "manifest.akmf", false);

        sst::SSTManager::Options sopts;
        sopts.sst_dir = dir / "sst";
        sopts.max_l0_files = 100;
        sopts.block_size = 4096;
        auto manager = sst::SSTManager::create(sopts, manifest.get());
        manager->recover();

        auto make_view = [](std::vector<uint8_t>& key, std::vector<uint8_t>& value, uint64_t seq, uint8_t flags) {
            const uint64_t fp = core::compute_key_fp64(key.data(), key.size());
            const uint64_t mk = core::build_mini_key(key.data(), key.size());
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
        batch1.push_back(make_view(a, va, 1, core::SSTHdr32::FLAG_NORMAL));
        batch1.push_back(make_view(b, vb1, 2, core::SSTHdr32::FLAG_NORMAL));
        batch1.push_back(make_view(c, vc1, 3, core::SSTHdr32::FLAG_NORMAL));
        manager->flush(batch1);

        auto vb2 = bytes("new-b");
        auto empty = bytes("");
        auto vd = bytes("new-d");
        std::vector<core::RecordView> batch2;
        batch2.push_back(make_view(b, vb2, 10, core::SSTHdr32::FLAG_NORMAL));
        batch2.push_back(make_view(c, empty, 11, core::SSTHdr32::FLAG_TOMBSTONE));
        batch2.push_back(make_view(d, vd, 12, core::SSTHdr32::FLAG_NORMAL));
        manager->flush(batch2);

        std::vector<std::string> keys;
        std::vector<std::string> values;
        {
            auto it = manager->scan_iter();
            while (it.has_next()) {
                auto rec = it.next();
                if (!rec) { throw std::runtime_error("scan_iter has_next returned true but next returned nullopt"); }
                keys.push_back(str(rec->key));
                values.push_back(str(rec->value));
            }
        }
        if (keys != std::vector<std::string>{"a", "b", "d"} || values != std::vector<std::string>{"old-a", "new-b", "new-d"}) {
            std::string msg = "scan_iter mismatch\nkeys:";
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
    test_writer_reader_roundtrip();
    test_memtable_flush_manager_recover();
    test_compaction_overwrite_and_tombstone();
    test_manager_scan_iter_merges_lazily();
    std::cout << "SST v2 smoke tests passed\n";
    return 0;
}

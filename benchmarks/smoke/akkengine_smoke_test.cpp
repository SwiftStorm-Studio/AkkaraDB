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

// benchmarks/smoke/akkengine_smoke_test.cpp
#include "TestErrorHandlers.hpp"

#include "akk/engine/AkkEngine.hpp"

#include <cassert>
#include <filesystem>
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

    [[nodiscard]] fs::path temp_dir(std::string_view name) {
        auto path = fs::temp_directory_path() / "akkaradb_akkengine_smoke" / std::string{name};
        fs::remove_all(path);
        fs::create_directories(path);
        return path;
    }

    void test_memory_basic() {
        AkkEngineOptions opts;
        opts.components.wal_enabled = false;
        opts.components.blob_enabled = false;
        opts.components.manifest_enabled = false;
        opts.components.sst_enabled = false;

        auto engine = AkkEngine::open(opts);
        engine->put(bytes("k"), bytes("v1"));
        assert(text(*engine->get(bytes("k"))) == "v1");
        assert(engine->exists(bytes("k")));

        engine->put(bytes("k"), bytes("v2"));
        std::vector<uint8_t> out;
        assert(engine->get_into(bytes("k"), out));
        assert(text(out) == "v2");

        engine->remove(bytes("k"));
        assert(!engine->get(bytes("k")));
        assert(!engine->exists(bytes("k")));

        engine->put(bytes("a"), bytes("1"));
        engine->put(bytes("b"), bytes("2"));
        assert(engine->count(bytes("a"), bytes("c")) == 2);

        const auto stats = engine->stats();
        assert(stats.puts_total >= 4);
        assert(stats.removes_total >= 1);
        assert(stats.gets_total >= 3);
        assert(stats.exists_total >= 2);
        assert(stats.memtable.shard_count > 0);

        engine->close();
        engine->close();
    }

    void test_wal_recovery() {
        const auto dir = temp_dir("wal");

        {
            AkkEngineOptions opts;
            opts.paths.data_dir = dir;
            opts.components.blob_enabled = false;
            opts.components.manifest_enabled = false;
            opts.components.sst_enabled = false;
            auto engine = AkkEngine::open(opts);
            engine->put(bytes("a"), bytes("1"));
            engine->force_sync();
        }

        {
            AkkEngineOptions opts;
            opts.paths.data_dir = dir;
            opts.components.blob_enabled = false;
            opts.components.manifest_enabled = false;
            opts.components.sst_enabled = false;
            auto engine = AkkEngine::open(opts);
            assert(text(*engine->get(bytes("a"))) == "1");
        }
    }

    void test_blob() {
        const auto dir = temp_dir("blob");
        AkkEngineOptions opts;
        opts.paths.data_dir = dir;
        opts.components.manifest_enabled = false;
        opts.components.sst_enabled = false;
        opts.blob.threshold_bytes = 4;

        auto engine = AkkEngine::open(opts);
        engine->put(bytes("blob"), bytes("large-value"));
        assert(text(*engine->get(bytes("blob"))) == "large-value");
    }

    void test_flush_and_scan() {
        const auto dir = temp_dir("flush");
        AkkEngineOptions opts;
        opts.paths.data_dir = dir;
        opts.components.blob_enabled = false;
        opts.memtable.threshold_bytes_per_shard = 1;

        auto engine = AkkEngine::open(opts);
        engine->put(bytes("a"), bytes("1"));
        engine->put(bytes("b"), bytes("2"));
        engine->remove(bytes("a"));
        engine->force_flush();

        assert(!engine->get(bytes("a")));
        assert(text(*engine->get(bytes("b"))) == "2");

        akkaradb::core::BufferArena scan_arena;
        auto rows = engine->scan(scan_arena);
        auto it = rows.begin();
        int count = 0;
        while (!(it == rows.end())) {
            const auto& item = *it;
            assert(text(item.key) == "b");
            assert(text(item.value) == "2");
            ++count;
            ++it;
        }
        assert(count == 1);
    }

    void test_version_log() {
        const auto dir = temp_dir("vlog");
        AkkEngineOptions opts;
        opts.paths.data_dir = dir;
        opts.components.blob_enabled = false;
        opts.components.manifest_enabled = false;
        opts.components.sst_enabled = false;
        opts.components.version_log_enabled = true;

        auto engine = AkkEngine::open(opts);
        engine->put(bytes("v"), bytes("one"));
        engine->put(bytes("v"), bytes("two"));
        const auto hist = engine->history(bytes("v"));
        assert(hist.size() == 2);
        assert(text(*engine->get_at(bytes("v"), hist[0].seq)) == "one");
        engine->rollback_key(bytes("v"), hist[0].seq);
        assert(text(*engine->get(bytes("v"))) == "one");
    }
}

int main() {
    akkara::test::install_msvc_test_error_handlers();

    test_memory_basic();
    test_wal_recovery();
    test_blob();
    test_flush_and_scan();
    test_version_log();
    return 0;
}

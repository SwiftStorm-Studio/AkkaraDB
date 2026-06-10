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
    static std::filesystem::path make_temp_dir(const std::string& suffix) {
        auto dir = std::filesystem::temp_directory_path() / ("akkaradb_manifest_smoke_" + suffix);
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
        std::filesystem::create_directories(dir, ec);
        assert(!ec);
        return dir;
    }

    static std::filesystem::path latest_manifest_file(const std::filesystem::path& base) {
        if (std::filesystem::exists(base)) {
            return base;
        }
        for (size_t i = 1; i < 10000; ++i) {
            auto rotated = base.parent_path() / (base.filename().string() + "." + std::to_string(i));
            if (std::filesystem::exists(rotated)) {
                return rotated;
            }
        }
        return base;
    }

    static void test_basic_state_and_replay() {
        const auto dir = make_temp_dir("basic");
        const auto path = dir / "manifest.akmf";

        {
            auto mf = Manifest::create(path, false);
            mf->start();
            mf->advance(42);
            mf->sst_seal(0, "L0_0001.aksst", 100, std::optional<std::string>{"aa"}, std::optional<std::string>{"ff"});
            mf->sst_delete("L0_0001.aksst");
            mf->sst_seal(0, "L0_0002.aksst", 200, std::nullopt, std::nullopt);
            mf->checkpoint(std::optional<std::string>{"cp1"}, std::optional<uint64_t>{42}, std::optional<uint64_t>{1000});
            mf->close();
        }

        {
            auto mf = Manifest::create(path, false);
            const auto live = mf->live_sst();
            assert(live.size() == 1);
            assert(live[0] == "L0_0002.aksst");

            const auto deleted = mf->deleted_sst();
            assert(deleted.empty());

            const auto cp = mf->last_checkpoint();
            assert(cp.has_value());
            assert(cp->name.has_value() && cp->name.value() == "cp1");
            assert(cp->stripe.has_value() && cp->stripe.value() == 42);
            assert(cp->last_seq.has_value() && cp->last_seq.value() == 1000);
            assert(mf->stripes_written() == 42);
        }
    }

    static void test_compaction_commit() {
        const auto dir = make_temp_dir("commit");
        const auto path = dir / "manifest.akmf";

        auto mf = Manifest::create(path, false);
        mf->sst_seal(0, "in1.aksst", 10, std::nullopt, std::nullopt);
        mf->sst_seal(0, "in2.aksst", 20, std::nullopt, std::nullopt);
        mf->compaction_commit({"out1.aksst", "out2.aksst"}, {"in1.aksst", "in2.aksst"});

        const auto live = mf->live_sst();
        bool has_out1 = false;
        bool has_out2 = false;
        bool has_in1 = false;
        for (const auto& f : live) {
            if (f == "out1.aksst") has_out1 = true;
            if (f == "out2.aksst") has_out2 = true;
            if (f == "in1.aksst") has_in1 = true;
        }
        assert(has_out1 && has_out2);
        assert(!has_in1);
    }

    static void test_crc_stop_on_corruption() {
        const auto dir = make_temp_dir("crc");
        const auto path = dir / "manifest.akmf";

        {
            auto mf = Manifest::create(path, false);
            mf->sst_seal(0, "a.aksst", 1, std::nullopt, std::nullopt);
            mf->sst_seal(0, "b.aksst", 1, std::nullopt, std::nullopt);
            mf->close();
        }

        {
            std::fstream file(latest_manifest_file(path), std::ios::in | std::ios::out | std::ios::binary);
            assert(file.good());
            // Corrupt first record payload byte after 32B file header + 8B rec header.
            file.seekp(40, std::ios::beg);
            char bad = '\x7f';
            file.write(&bad, 1);
        }

        {
            auto mf = Manifest::create(path, false);
            // replay stops at first CRC mismatch; no valid SST state should be rebuilt.
            assert(mf->live_sst().empty());
        }
    }
} // namespace

int main() {
    akkara::test::install_msvc_test_error_handlers();

    test_basic_state_and_replay();
    test_compaction_commit();
    test_crc_stop_on_corruption();
    std::printf("manifest smoke test passed\n");
    return 0;
}

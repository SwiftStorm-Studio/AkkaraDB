/*
 * AkkaraDB - VersionLog smoke test
 */

#include "akk/engine/vlog/VersionLog.hpp"

#include <cassert>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <vector>

using namespace akkaradb::engine::vlog;

namespace {
    static std::span<const uint8_t> as_u8(std::string_view sv) {
        return {reinterpret_cast<const uint8_t*>(sv.data()), sv.size()};
    }

    static std::string to_string(std::span<const uint8_t> value) {
        return {reinterpret_cast<const char*>(value.data()), value.size()};
    }

    static std::filesystem::path make_temp_dir(const std::string& suffix) {
        auto dir = std::filesystem::temp_directory_path() / ("akkaradb_vlog_smoke_" + suffix);
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
        std::filesystem::create_directories(dir, ec);
        assert(!ec);
        return dir;
    }

    static void test_append_and_queries() {
        const auto dir = make_temp_dir("append_queries");
        const auto path = dir / "history.akvlog";

        auto log = VersionLog::create(VersionLogOptions{.log_path = path, .sync_mode = VLogSyncMode::Async});
        log->append(as_u8("k"), 10, 7, 1000, 0x00, as_u8("v1"));
        log->append(as_u8("k"), 20, 7, 2000, 0x00, as_u8("v2"));
        log->append(as_u8("k"), 30, 7, 3000, 0x01, {});

        {
            const auto h = log->history(as_u8("k"));
            assert(h.size() == 3);
            assert(h[0].seq == 10);
            assert(h[1].seq == 20);
            assert(h[2].seq == 30);
        }

        {
            const auto v = log->get_at(as_u8("k"), 5);
            assert(!v.has_value());
        }
        {
            const auto v = log->get_at(as_u8("k"), 20);
            assert(v.has_value());
            assert(to_string(v->value) == "v2");
        }
        {
            const auto v = log->get_at(as_u8("k"), 999);
            assert(v.has_value());
            assert(v->flags == 0x01);
        }
    }

    static void test_recovery_and_corruption_tolerance() {
        const auto dir = make_temp_dir("recovery");
        const auto path = dir / "history.akvlog";

        {
            auto log = VersionLog::create(VersionLogOptions{.log_path = path, .sync_mode = VLogSyncMode::Sync});
            log->append(as_u8("a"), 1, 1, 100, 0x00, as_u8("x"));
            log->append(as_u8("a"), 2, 1, 200, 0x00, as_u8("y"));
            log->append(as_u8("b"), 3, 1, 300, 0x00, as_u8("z"));
            log->close();
        }

        {
            auto log = VersionLog::create(VersionLogOptions{.log_path = path, .sync_mode = VLogSyncMode::Async});
            const auto va = log->get_at(as_u8("a"), 999);
            assert(va.has_value());
            assert(to_string(va->value) == "y");
        }

        {
            std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
            assert(file.good());
            file.seekp(40, std::ios::beg);
            char bad = '\x7f';
            file.write(&bad, 1);
        }

        {
            auto log = VersionLog::create(VersionLogOptions{.log_path = path, .sync_mode = VLogSyncMode::Async});
            const auto vb = log->get_at(as_u8("b"), 999);
            (void)vb;
        }
    }

    static void test_collect_rollback_targets() {
        const auto dir = make_temp_dir("rollback_targets");
        const auto path = dir / "history.akvlog";

        auto log = VersionLog::create(VersionLogOptions{.log_path = path, .sync_mode = VLogSyncMode::Async});
        log->append(as_u8("k1"), 10, 1, 10, 0x00, as_u8("v1"));
        log->append(as_u8("k1"), 40, ROLLBACK_NODE, 40, static_cast<uint8_t>(0x00 | VLOG_FLAG_ROLLBACK), as_u8("v2"));
        log->append(as_u8("k2"), 20, 1, 20, 0x00, as_u8("x1"));

        const auto targets = log->collect_rollback_targets(25);
        assert(targets.size() == 1);
        assert(std::string(targets[0].first.begin(), targets[0].first.end()) == "k1");
        assert(targets[0].second.has_value());
        assert(targets[0].second->seq == 10);
    }
} // namespace

int main() {
    test_append_and_queries();
    test_recovery_and_corruption_tolerance();
    test_collect_rollback_targets();
    std::printf("versionlog smoke test passed\n");
    return 0;
}

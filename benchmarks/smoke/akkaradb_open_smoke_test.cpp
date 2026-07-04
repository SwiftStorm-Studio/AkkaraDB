#include "TestErrorHandlers.hpp"

#include "akkaradb/AkkaraDB.hpp"

#include <filesystem>
#include <system_error>

namespace fs = std::filesystem;

namespace {
    struct TempDir {
        fs::path path;

        explicit TempDir(const char* name) : path(fs::temp_directory_path() / "akkaradbOpenSmoke" / name) {
            std::error_code ec;
            fs::remove_all(path, ec);
            fs::create_directories(path, ec);
            AKK_TEST_CHECK(!ec);
        }

        ~TempDir() {
            std::error_code ec;
            fs::remove_all(path, ec);
        }
    };

    void testOpenNormalMode() {
        TempDir dir{"normal"};
        auto db = akkaradb::AkkaraDB::open(dir.path, akkaradb::StartupMode::NORMAL);
        AKK_TEST_CHECK(fs::exists(dir.path / "manifest.akmf"));
        AKK_TEST_CHECK(fs::exists(dir.path / "node.id"));
        db->close();
    }

    void testOpenDurableWithOverrides() {
        TempDir dir{"durable"};
        akkaradb::AkkaraDB::Options opts;
        opts.dataDir = dir.path;
        opts.mode = akkaradb::StartupMode::DURABLE;
        opts.overrides.memtableThresholdPerShard = 4ULL * 1024ULL * 1024ULL;
        opts.overrides.versionLogEnabled = true;
        opts.overrides.sstCodec = akkaradb::engine::Codec::NONE;
        opts.overrides.blobCodec = akkaradb::engine::Codec::NONE;
        opts.overrides.blobThresholdBytes = 8ULL * 1024ULL;
        opts.overrides.sstPromoteReads = true;
        opts.overrides.sstBloomBitsPerKey = 12;
        opts.overrides.maxL0SstFiles = 8;

        auto db = akkaradb::AkkaraDB::open(std::move(opts));
        AKK_TEST_CHECK(fs::exists(dir.path / "manifest.akmf"));
        AKK_TEST_CHECK(fs::exists(dir.path / "history.akvlog"));
        AKK_TEST_CHECK(fs::exists(dir.path / "node.id"));
        db->close();
    }
}

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();
    testOpenNormalMode();
    testOpenDurableWithOverrides();
    return 0;
}

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

// benchmarks/smoke/blob_smoke_test.cpp
#include "TestErrorHandlers.hpp"

#include "akk/engine/blob/BlobManager.hpp"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace akkaradb::engine::blob;

namespace {
    static std::filesystem::path make_temp_dir(const std::string& suffix) {
        auto dir = std::filesystem::temp_directory_path() / ("akkaradb_blob_smoke_" + suffix);
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
        std::filesystem::create_directories(dir, ec);
        assert(!ec);
        return dir;
    }

    static std::vector<uint8_t> patterned_payload(size_t size) {
        std::vector<uint8_t> payload(size);
        for (size_t i = 0; i < payload.size(); ++i) { payload[i] = static_cast<uint8_t>((i * 131u) & 0xffu); }
        return payload;
    }

    static std::vector<uint8_t> pseudo_random_payload(size_t size) {
        std::vector<uint8_t> payload(size);
        uint64_t state = 0x9e3779b97f4a7c15ULL;
        for (auto& byte : payload) {
            state ^= state >> 12u;
            state ^= state << 25u;
            state ^= state >> 27u;
            byte = static_cast<uint8_t>((state * 0x2545f4914f6cdd1dULL) >> 56u);
        }
        return payload;
    }

    static AkBlobHeaderV5 read_header(const std::filesystem::path& path) {
        std::ifstream in(path, std::ios::binary);
        assert(in);
        uint8_t buf[AKBLOB_HEADER_SIZE_V5]{};
        in.read(reinterpret_cast<char*>(buf), sizeof(buf));
        assert(in.gcount() == static_cast<std::streamsize>(sizeof(buf)));
        return deserialize_blob_header(buf);
    }

    static bool wait_until_missing(const std::filesystem::path& path) {
        for (int i = 0; i < 100; ++i) {
            if (!std::filesystem::exists(path)) { return true; }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return !std::filesystem::exists(path);
    }

    static void test_blob_ref_roundtrip() {
        uint8_t buf[BLOB_REF_SIZE]{};
        const BlobRef in{0x0102030405060708ULL, 64ULL * 1024ULL, 0xaabbccddu};
        encode_blob_ref(buf, in);
        const BlobRef out = decode_blob_ref(buf);
        assert(out.blob_id == in.blob_id);
        assert(out.total_size == in.total_size);
        assert(out.content_crc32c == in.content_crc32c);
        static_assert(BLOB_REF_SIZE == 20);
    }

    static void test_roundtrip_and_reopen() {
        const auto dir = make_temp_dir("roundtrip");
        const uint64_t blob_id = 0x0000000000000042ULL;
        const auto payload = patterned_payload(64 * 1024);
        std::filesystem::path path;

        {
            auto mgr = BlobManager::create(BlobManager::Options{dir});
            mgr->start();
            mgr->write(blob_id, payload);
            path = mgr->blob_path(blob_id);
            assert(path.extension() == ".akblob");
            assert(std::filesystem::exists(path));
            assert(mgr->read(blob_id) == payload);
            mgr->close();
        }

        {
            auto mgr = BlobManager::create(BlobManager::Options{dir});
            mgr->start();
            assert(mgr->read(blob_id) == payload);
            mgr->close();
        }

        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    static void test_zstd_and_incompressible_paths() {
        const auto dir = make_temp_dir("zstd");
        const uint64_t zstd_id = 0x0100000000000001ULL;
        const uint64_t raw_id = 0x0100000000000002ULL;
        std::vector<uint8_t> compressible(64 * 1024, 0x3a);
        const auto incompressible = pseudo_random_payload(64 * 1024);

        auto mgr = BlobManager::create(BlobManager::Options{dir, DEFAULT_THRESHOLD_BYTES, BlobCodec::Zstd});
        mgr->start();
        mgr->write(zstd_id, compressible);
        mgr->write(raw_id, incompressible);

        const auto zstd_hdr = read_header(mgr->blob_path(zstd_id));
        assert(verify_blob_header(zstd_hdr));
        assert(zstd_hdr.codec == static_cast<uint32_t>(BlobCodec::Zstd));
        assert((zstd_hdr.flags & AKBLOB_FLAG_ZSTD) != 0);
        assert(zstd_hdr.stored_size < zstd_hdr.total_size);
        assert(mgr->read(zstd_id) == compressible);

        const auto raw_hdr = read_header(mgr->blob_path(raw_id));
        assert(verify_blob_header(raw_hdr));
        assert(raw_hdr.codec == static_cast<uint32_t>(BlobCodec::None));
        assert((raw_hdr.flags & AKBLOB_FLAG_ZSTD) == 0);
        assert(raw_hdr.stored_size == raw_hdr.total_size);
        assert(mgr->read(raw_id) == incompressible);

        mgr->close();
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    static void test_gc_delete_and_orphans() {
        const auto dir = make_temp_dir("gc");
        const auto payload = patterned_payload(32 * 1024);

        auto mgr = BlobManager::create(BlobManager::Options{dir});
        mgr->start();

        mgr->write(0x0200000000000001ULL, payload);
        const auto deleted_path = mgr->blob_path(0x0200000000000001ULL);
        mgr->schedule_delete(0x0200000000000001ULL);
        assert(wait_until_missing(deleted_path));

        mgr->write(0x0200000000000002ULL, payload);
        mgr->write(0x0200000000000003ULL, payload);
        const auto kept_path = mgr->blob_path(0x0200000000000002ULL);
        const auto orphan_path = mgr->blob_path(0x0200000000000003ULL);
        mgr->scan_orphans([](uint64_t id) { return id == 0x0200000000000002ULL; });
        assert(wait_until_missing(orphan_path));
        assert(std::filesystem::exists(kept_path));

        mgr->close();
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    static void test_startup_cleanup() {
        const auto dir = make_temp_dir("cleanup");
        const auto shard = dir / "00";
        std::filesystem::create_directories(shard);
        const auto tmp = shard / "0000000000000001.akblob.tmp";
        const auto del = shard / "0000000000000002.akblob.del";
        {
            std::ofstream(tmp, std::ios::binary) << "tmp";
            std::ofstream(del, std::ios::binary) << "del";
        }
        assert(std::filesystem::exists(tmp));
        assert(std::filesystem::exists(del));

        auto mgr = BlobManager::create(BlobManager::Options{dir});
        mgr->start();
        assert(!std::filesystem::exists(tmp));
        assert(!std::filesystem::exists(del));
        mgr->close();

        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    static void test_content_crc_failure() {
        const auto dir = make_temp_dir("corrupt");
        const uint64_t blob_id = 0x0300000000000001ULL;
        const auto payload = patterned_payload(32 * 1024);

        auto mgr = BlobManager::create(BlobManager::Options{dir});
        mgr->start();
        mgr->write(blob_id, payload);
        const auto path = mgr->blob_path(blob_id);
        {
            std::fstream io(path, std::ios::binary | std::ios::in | std::ios::out);
            assert(io);
            io.seekg(-1, std::ios::end);
            char c = 0;
            io.read(&c, 1);
            io.seekp(-1, std::ios::end);
            c = static_cast<char>(c ^ 0x7f);
            io.write(&c, 1);
        }

        bool threw = false;
        try {
            (void)mgr->read(blob_id);
        }
        catch (const std::runtime_error&) {
            threw = true;
        }
        assert(threw);

        mgr->close();
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }
} // namespace

int main() {
    akkara::test::install_msvc_test_error_handlers();

    test_blob_ref_roundtrip();
    test_roundtrip_and_reopen();
    test_zstd_and_incompressible_paths();
    test_gc_delete_and_orphans();
    test_startup_cleanup();
    test_content_crc_failure();
    std::printf("akkaradb_blob_smoke_test: ok\n");
    return 0;
}

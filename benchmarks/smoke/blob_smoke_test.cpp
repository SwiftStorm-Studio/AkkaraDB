/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
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
    static std::filesystem::path makeTempDir(const std::string& suffix) {
        auto dir = std::filesystem::temp_directory_path() / ("akkaradbBlobSmoke_" + suffix);
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
        std::filesystem::create_directories(dir, ec);
        AKK_TEST_CHECK(!ec);
        return dir;
    }

    static std::vector<uint8_t> patternedPayload(size_t size) {
        std::vector<uint8_t> payload(size);
        for (size_t i = 0; i < payload.size(); ++i) { payload[i] = static_cast<uint8_t>((i * 131u) & 0xffu); }
        return payload;
    }

    static std::vector<uint8_t> pseudoRandomPayload(size_t size) {
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

    static AkBlobHeaderV5 readHeader(const std::filesystem::path& path) {
        std::ifstream in(path, std::ios::binary);
        AKK_TEST_CHECK(in);
        uint8_t buf[AKBLOB_HEADER_SIZE_V5]{};
        in.read(reinterpret_cast<char*>(buf), sizeof(buf));
        AKK_TEST_CHECK(in.gcount() == static_cast<std::streamsize>(sizeof(buf)));
        return deserializeBlobHeader(buf);
    }

    static bool waitUntilMissing(const std::filesystem::path& path) {
        for (int i = 0; i < 100; ++i) {
            if (!std::filesystem::exists(path)) { return true; }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return !std::filesystem::exists(path);
    }

    static void testBlobRefRoundtrip() {
        uint8_t buf[BLOB_REF_SIZE]{};
        const BlobRef in{0x0102030405060708ULL, 64ULL * 1024ULL, 0xaabbccddu};
        encodeBlobRef(buf, in);
        const BlobRef out = decodeBlobRef(buf);
        AKK_TEST_CHECK(out.blobId == in.blobId);
        AKK_TEST_CHECK(out.totalSize == in.totalSize);
        AKK_TEST_CHECK(out.contentCrc32c == in.contentCrc32c);
        static_assert(BLOB_REF_SIZE == 20);
    }

    static void testRoundtripAndReopen() {
        const auto dir = makeTempDir("roundtrip");
        const uint64_t blobId = 0x0000000000000042ULL;
        const auto payload = patternedPayload(64 * 1024);
        std::filesystem::path path;

        {
            auto mgr = BlobManager::create(BlobManager::Options{dir});
            mgr->start();
            mgr->write(blobId, payload);
            path = mgr->blobPath(blobId);
            AKK_TEST_CHECK(path.extension() == ".akblob");
            AKK_TEST_CHECK(std::filesystem::exists(path));
            AKK_TEST_CHECK(mgr->read(blobId) == payload);
            mgr->close();
        }

        {
            auto mgr = BlobManager::create(BlobManager::Options{dir});
            mgr->start();
            AKK_TEST_CHECK(mgr->read(blobId) == payload);
            mgr->close();
        }

        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    static void testZstdAndIncompressiblePaths() {
        const auto dir = makeTempDir("zstd");
        const uint64_t zstdId = 0x0100000000000001ULL;
        const uint64_t rawId = 0x0100000000000002ULL;
        std::vector<uint8_t> compressible(64 * 1024, 0x3a);
        const auto incompressible = pseudoRandomPayload(64 * 1024);

        auto mgr = BlobManager::create(BlobManager::Options{dir, DEFAULT_THRESHOLD_BYTES, BlobCodec::ZSTD});
        mgr->start();
        mgr->write(zstdId, compressible);
        mgr->write(rawId, incompressible);

        const auto zstdHdr = readHeader(mgr->blobPath(zstdId));
        AKK_TEST_CHECK(verifyBlobHeader(zstdHdr));
        AKK_TEST_CHECK(zstdHdr.codec == static_cast<uint32_t>(BlobCodec::ZSTD));
        AKK_TEST_CHECK((zstdHdr.flags & AKBLOB_FLAG_ZSTD) != 0);
        AKK_TEST_CHECK(zstdHdr.storedSize < zstdHdr.totalSize);
        AKK_TEST_CHECK(mgr->read(zstdId) == compressible);

        const auto rawHdr = readHeader(mgr->blobPath(rawId));
        AKK_TEST_CHECK(verifyBlobHeader(rawHdr));
        AKK_TEST_CHECK(rawHdr.codec == static_cast<uint32_t>(BlobCodec::NONE));
        AKK_TEST_CHECK((rawHdr.flags & AKBLOB_FLAG_ZSTD) == 0);
        AKK_TEST_CHECK(rawHdr.storedSize == rawHdr.totalSize);
        AKK_TEST_CHECK(mgr->read(rawId) == incompressible);

        mgr->close();
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    static void testGcDeleteAndOrphans() {
        const auto dir = makeTempDir("gc");
        const auto payload = patternedPayload(32 * 1024);

        auto mgr = BlobManager::create(BlobManager::Options{dir});
        mgr->start();

        mgr->write(0x0200000000000001ULL, payload);
        const auto deletedPath = mgr->blobPath(0x0200000000000001ULL);
        mgr->scheduleDelete(0x0200000000000001ULL);
        AKK_TEST_CHECK(waitUntilMissing(deletedPath));

        mgr->write(0x0200000000000002ULL, payload);
        mgr->write(0x0200000000000003ULL, payload);
        const auto keptPath = mgr->blobPath(0x0200000000000002ULL);
        const auto orphanPath = mgr->blobPath(0x0200000000000003ULL);
        mgr->scanOrphans([](uint64_t id) { return id == 0x0200000000000002ULL; });
        AKK_TEST_CHECK(waitUntilMissing(orphanPath));
        AKK_TEST_CHECK(std::filesystem::exists(keptPath));

        mgr->close();
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    static void testStartupCleanup() {
        const auto dir = makeTempDir("cleanup");
        const auto shard = dir / "00";
        std::filesystem::create_directories(shard);
        const auto tmp = shard / "0000000000000001.akblob.tmp";
        const auto del = shard / "0000000000000002.akblob.del";
        {
            std::ofstream(tmp, std::ios::binary) << "tmp";
            std::ofstream(del, std::ios::binary) << "del";
        }
        AKK_TEST_CHECK(std::filesystem::exists(tmp));
        AKK_TEST_CHECK(std::filesystem::exists(del));

        auto mgr = BlobManager::create(BlobManager::Options{dir});
        mgr->start();
        AKK_TEST_CHECK(!std::filesystem::exists(tmp));
        AKK_TEST_CHECK(!std::filesystem::exists(del));
        mgr->close();

        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    static void testContentCrcFailure() {
        const auto dir = makeTempDir("corrupt");
        const uint64_t blobId = 0x0300000000000001ULL;
        const auto payload = patternedPayload(32 * 1024);

        auto mgr = BlobManager::create(BlobManager::Options{dir});
        mgr->start();
        mgr->write(blobId, payload);
        const auto path = mgr->blobPath(blobId);
        {
            std::fstream io(path, std::ios::binary | std::ios::in | std::ios::out);
            AKK_TEST_CHECK(io);
            io.seekg(-1, std::ios::end);
            char c = 0;
            io.read(&c, 1);
            io.seekp(-1, std::ios::end);
            c = static_cast<char>(c ^ 0x7f);
            io.write(&c, 1);
        }

        bool threw = false;
        try {
            (void)mgr->read(blobId);
        }
        catch (const std::runtime_error&) {
            threw = true;
        }
        AKK_TEST_CHECK(threw);

        mgr->close();
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }
} // namespace

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();

    testBlobRefRoundtrip();
    testRoundtripAndReopen();
    testZstdAndIncompressiblePaths();
    testGcDeleteAndOrphans();
    testStartupCleanup();
    testContentCrcFailure();
    std::printf("akkaradbBlobSmokeTest: ok\n");
    return 0;
}
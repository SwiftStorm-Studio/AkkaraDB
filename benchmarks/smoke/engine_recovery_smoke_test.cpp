/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "akk/engine/AkkEngine.hpp"
#include "akk/engine/manifest/Manifest.hpp"
#include "akk/engine/wal/WalWriter.hpp"

#include "TestErrorHandlers.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

namespace {
    namespace fs = std::filesystem;

    void require(bool condition, const char* message) {
        if (!condition) { throw std::runtime_error(message); }
    }

    [[nodiscard]] std::span<const uint8_t> bytes(const std::vector<uint8_t>& value) noexcept {
        return {value.data(), value.size()};
    }

    [[nodiscard]] std::span<const uint8_t> bytes(std::string_view value) noexcept {
        return {reinterpret_cast<const uint8_t*>(value.data()), value.size()};
    }

    template <typename Predicate>
    [[nodiscard]] bool waitUntil(Predicate predicate, std::chrono::milliseconds timeout = std::chrono::milliseconds{5000}) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (predicate()) { return true; }
            std::this_thread::sleep_for(std::chrono::milliseconds{25});
        }
        return predicate();
    }

    [[nodiscard]] akkaradb::engine::AkkEngineOptions persistentOptions(const fs::path& dir) {
        namespace memtable = akkaradb::engine::memtable;
        namespace sst = akkaradb::engine::sst;

        akkaradb::engine::AkkEngineOptions options;
        options.paths.dataDir = dir;
        options.components.walEnabled = true;
        options.components.blobEnabled = false;
        options.components.manifestEnabled = true;
        options.components.sstEnabled = true;
        options.components.versionLogEnabled = false;
        options.memtable.shardCount = 1;
        options.memtable.flushMode = memtable::MemTableFlushMode::MANUAL_ONLY;
        options.memtable.thresholdBytesPerShard = 0;
        options.sst.compactThreads = 0;
        options.sst.compactionMode = sst::SSTCompactionMode::DISABLED;
        options.runtime.forceFlushOnClose = true;
        options.runtime.forceSyncOnClose = true;
        return options;
    }

    [[nodiscard]] akkaradb::engine::AkkEngineOptions parallelWalOptions(
        const fs::path& dir,
        akkaradb::engine::AkkEngineOptions::WriteVisibilityMode visibility =
            akkaradb::engine::AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER,
        akkaradb::engine::wal::WalExecutionMode walExecution = akkaradb::engine::wal::WalExecutionMode::ASYNC
    ) {
        namespace wal = akkaradb::engine::wal;

        auto options = persistentOptions(dir);
        options.runtime.writeAdmission = akkaradb::engine::AkkEngineOptions::WriteAdmissionMode::PARALLEL;
        options.runtime.writePolicy = akkaradb::engine::AkkEngineOptions::WritePolicyPreset::CUSTOM;
        options.runtime.writeDurability = walExecution == wal::WalExecutionMode::ASYNC
                                              ? akkaradb::engine::AkkEngineOptions::WriteDurabilityMode::WRITTEN
                                              : akkaradb::engine::AkkEngineOptions::WriteDurabilityMode::SYNCED;
        options.runtime.writeVisibility = visibility;
        options.runtime.visibility.readVisibility = visibility == akkaradb::engine::AkkEngineOptions::WriteVisibilityMode::COMMIT_ORDER
                                                        ? akkaradb::engine::AkkEngineOptions::ReadVisibilityMode::COMMIT_ORDER
                                                        : akkaradb::engine::AkkEngineOptions::ReadVisibilityMode::APPLIED;
        options.wal.execution = walExecution;
        options.wal.syncPolicy = walExecution == wal::WalExecutionMode::ASYNC ? wal::WalSyncPolicy::ON_SYNC_ACK : wal::WalSyncPolicy::ALWAYS;
        options.wal.backpressure = wal::WalBackpressureMode::BLOCK;
        options.wal.groupN = 64;
        options.wal.groupMicros = 100;
        return options;
    }

    struct MatrixData {
        std::vector<uint8_t> alphaKey{'m', 'a', 't', 'r', 'i', 'x', '-', 'a'};
        std::vector<uint8_t> betaKey{'m', 'a', 't', 'r', 'i', 'x', '-', 'b'};
        std::vector<uint8_t> gammaKey{'m', 'a', 't', 'r', 'i', 'x', '-', 'c'};
        std::vector<uint8_t> alphaOld{'a', 'l', 'p', 'h', 'a', '-', 'o', 'l', 'd'};
        std::vector<uint8_t> alphaNew{'a', 'l', 'p', 'h', 'a', '-', 'n', 'e', 'w'};
        std::vector<uint8_t> betaValue{'b', 'e', 't', 'a'};
        std::vector<uint8_t> gammaValue{'g', 'a', 'm', 'm', 'a'};
    };

    void writeMatrixRecords(akkaradb::engine::AkkEngine& engine, const MatrixData& data) {
        engine.put(bytes(data.alphaKey), bytes(data.alphaOld));
        const std::vector<akkaradb::engine::AkkEngine::BatchPutEntry> batch{
            akkaradb::engine::AkkEngine::BatchPutEntry{bytes(data.betaKey), bytes(data.betaValue)},
            akkaradb::engine::AkkEngine::BatchPutEntry{bytes(data.gammaKey), bytes(data.gammaValue)},
        };
        engine.putBatch(batch);
        engine.remove(bytes(data.gammaKey));
        engine.put(bytes(data.alphaKey), bytes(data.alphaNew));
    }

    void requireMatrixRecords(akkaradb::engine::AkkEngine& engine, const MatrixData& data) {
        const auto alpha = engine.get(bytes(data.alphaKey));
        require(alpha.has_value() && *alpha == data.alphaNew, "parallel WAL recovery did not preserve overwritten value");
        const auto beta = engine.get(bytes(data.betaKey));
        require(beta.has_value() && *beta == data.betaValue, "parallel WAL recovery did not preserve batch value");
        const auto gamma = engine.get(bytes(data.gammaKey));
        require(!gamma.has_value(), "parallel WAL recovery did not preserve remove tombstone");
        require(engine.stats().currentSeq == 5, "parallel WAL recovery did not restore commit-order sequence");
    }

    int runCrashWriter(const fs::path& dir) {
        const auto engine = akkaradb::engine::AkkEngine::open(persistentOptions(dir));
        const std::vector<uint8_t> key{'c', 'r', 'a', 's', 'h', '-', 'k', 'e', 'y'};
        const std::vector<uint8_t> value{'c', 'r', 'a', 's', 'h', '-', 'v', 'a', 'l', 'u', 'e'};
        engine->put(bytes(key), bytes(value));
        engine->forceSync();
        std::quick_exit(0);
    }

    int runParallelWalCrashWriter(const fs::path& dir) {
        const auto engine = akkaradb::engine::AkkEngine::open(parallelWalOptions(dir));
        const MatrixData data;
        writeMatrixRecords(*engine, data);
        engine->forceSync();
        std::quick_exit(0);
    }

    [[nodiscard]] std::string quoteArgument(const fs::path& value) {
        return '\"' + value.string() + '\"';
    }

    [[nodiscard]] std::string quoteArgument(const std::string& value) {
        return '\"' + value + '\"';
    }

    void setCrashPoint(const std::string& point) {
#ifdef _WIN32
        if (_putenv_s("AKKARADB_TEST_CRASH_POINT", point.c_str()) != 0) {
            throw std::runtime_error("failed to set crash point");
        }
#else
        if (::setenv("AKKARADB_TEST_CRASH_POINT", point.c_str(), 1) != 0) {
            throw std::runtime_error("failed to set crash point");
        }
#endif
    }

    void setTestWalSegmentBytes(uint64_t bytes) {
        const std::string value = std::to_string(bytes);
#ifdef _WIN32
        if (_putenv_s("AKKARADB_TEST_WAL_SEGMENT_BYTES", value.c_str()) != 0) {
            throw std::runtime_error("failed to set test WAL segment size");
        }
#else
        if (::setenv("AKKARADB_TEST_WAL_SEGMENT_BYTES", value.c_str(), 1) != 0) {
            throw std::runtime_error("failed to set test WAL segment size");
        }
#endif
    }

    [[nodiscard]] int expectedCrashStatus() noexcept {
#ifdef _WIN32
        return 86;
#else
        return 86 << 8;
#endif
    }

    [[nodiscard]] std::string crashChildCommand(
        const fs::path& executable,
        const std::string& mode,
        const std::string& point,
        const fs::path& dir
    ) {
#ifdef _WIN32
        return "\"\"" + executable.string() + "\" " + mode + " " + quoteArgument(point) + " " + quoteArgument(dir) + "\"";
#else
        return quoteArgument(executable) + " " + mode + " " + quoteArgument(point) + " " + quoteArgument(dir);
#endif
    }

    void verifySingleFlushRecovery(const fs::path& dir, bool expectLiveSst, uint64_t expectedSeq, bool expectPrunedWalSegment) {
        const std::vector<uint8_t> key{'b', 'o', 'u', 'n', 'd', 'a', 'r', 'y', '-', 'k', 'e', 'y'};
        const std::vector<uint8_t> value{'b', 'o', 'u', 'n', 'd', 'a', 'r', 'y', '-', 'v', 'a', 'l', 'u', 'e'};
        const fs::path sstDir = dir / "sstable";
        auto options = persistentOptions(dir);
        if (expectPrunedWalSegment) { options.wal.shardCount = 1; }
        auto engine = akkaradb::engine::AkkEngine::open(std::move(options));
        const auto recovered = engine->get(bytes(key));
        require(recovered.has_value() && *recovered == value, "flush-boundary crash lost a durable record");
        require(engine->stats().currentSeq == expectedSeq, "flush-boundary recovery reused the durable sequence");

        size_t sstCount = 0;
        for (const auto& entry : fs::directory_iterator(sstDir)) {
            require(entry.path().extension() != ".tmp", "recovery did not remove an interrupted SST temporary file");
            if (entry.path().extension() == ".aksst") { ++sstCount; }
        }
        require(
            expectLiveSst ? sstCount == 1 : sstCount == 0,
            "recovery retained the wrong SST set for the interrupted flush boundary"
        );
        if (expectPrunedWalSegment) {
            size_t walCount = 0;
            std::string walName;
            for (const auto& entry : fs::directory_iterator(dir / "wal")) {
                if (entry.path().extension() == ".akwal") {
                    ++walCount;
                    walName = entry.path().filename().string();
                }
            }
            require(walCount == 1 && walName.ends_with("0001.akwal"), "WAL prune boundary did not remove the sealed segment");
        }
        engine->close();
    }

    int runFlushCrashWriter(const std::string& point, const fs::path& dir) {
        setCrashPoint(point);
        const bool exerciseWalPrune = point == "engine.flush.after_wal_prune";
        if (exerciseWalPrune) { setTestWalSegmentBytes(2000); }
        auto options = persistentOptions(dir);
        if (exerciseWalPrune) { options.wal.shardCount = 1; }
        const auto engine = akkaradb::engine::AkkEngine::open(std::move(options));
        const std::vector<uint8_t> key{'b', 'o', 'u', 'n', 'd', 'a', 'r', 'y', '-', 'k', 'e', 'y'};
        const std::vector<uint8_t> value{'b', 'o', 'u', 'n', 'd', 'a', 'r', 'y', '-', 'v', 'a', 'l', 'u', 'e'};
        if (exerciseWalPrune) {
            const std::vector<uint8_t> preludeKey{'p', 'r', 'u', 'n', 'e', '-', 'p', 'r', 'e', 'l', 'u', 'd', 'e'};
            const std::vector<uint8_t> preludeValue(1880, static_cast<uint8_t>('p'));
            engine->put(bytes(preludeKey), bytes(preludeValue));
        }
        engine->put(bytes(key), bytes(value));
        engine->forceFlush();
        return 42;
    }

    int runCompactionCrashWriter(const std::string& point, const fs::path& dir) {
        namespace sst = akkaradb::engine::sst;

        setCrashPoint(point);
        auto options = persistentOptions(dir);
        options.sst.compactionMode = sst::SSTCompactionMode::BACKGROUND;
        options.sst.compactThreads = 1;
        options.sst.maxL0Files = 2;
        const auto engine = akkaradb::engine::AkkEngine::open(options);

        const std::vector<uint8_t> alphaKey{'c', 'o', 'm', 'p', 'a', 'c', 't', '-', 'a'};
        const std::vector<uint8_t> betaKey{'c', 'o', 'm', 'p', 'a', 'c', 't', '-', 'b'};
        const std::vector<uint8_t> alphaValue{'a', 'l', 'p', 'h', 'a'};
        const std::vector<uint8_t> betaValue{'b', 'e', 't', 'a'};
        engine->put(bytes(alphaKey), bytes(alphaValue));
        engine->forceFlush();
        engine->put(bytes(betaKey), bytes(betaValue));
        engine->forceFlush();

        std::this_thread::sleep_for(std::chrono::seconds{5});
        return 42;
    }

    void runFlushBoundaryCrashRecoveryTests(const fs::path& executable) {
        struct Case { const char* point; bool expectLiveSst; uint64_t expectedSeq; bool expectPrunedWalSegment; };
        constexpr std::array cases{
            Case{"sst.flush.after_tmp_write", false, 1, false},
            Case{"sst.flush.after_rename", false, 1, false},
            Case{"sst.flush.after_manifest_seal", true, 1, false},
            Case{"engine.flush.after_sst_flush", true, 1, false},
            Case{"engine.flush.after_wal_prune", true, 2, true},
            Case{"engine.flush.after_manifest_checkpoint", true, 1, false},
        };

        for (const auto& test : cases) {
            const fs::path dir = fs::temp_directory_path() / ("akkaradb_flush_crash_" + std::string{test.point});
            std::error_code ec;
            fs::remove_all(dir, ec);
            fs::create_directories(dir, ec);
            require(!ec, "failed to create flush crash test directory");
            const int status = std::system(crashChildCommand(executable, "--flush-crash-writer", test.point, dir).c_str());
            require(status == expectedCrashStatus(), "flush crash writer did not terminate at the requested persistence boundary");
            verifySingleFlushRecovery(dir, test.expectLiveSst, test.expectedSeq, test.expectPrunedWalSegment);
            fs::remove_all(dir, ec);
        }
    }

    void runCompactionCrashRecoveryTests(const fs::path& executable) {
        struct Case { const char* point; bool expectCompactedOutput; };
        constexpr std::array cases{
            Case{"sst.compaction.after_output_tmp_write", false},
            Case{"sst.compaction.after_output_rename", false},
            Case{"sst.compaction.after_manifest_commit", true},
            Case{"sst.compaction.after_input_delete", true},
        };

        const std::vector<uint8_t> alphaKey{'c', 'o', 'm', 'p', 'a', 'c', 't', '-', 'a'};
        const std::vector<uint8_t> betaKey{'c', 'o', 'm', 'p', 'a', 'c', 't', '-', 'b'};
        const std::vector<uint8_t> alphaValue{'a', 'l', 'p', 'h', 'a'};
        const std::vector<uint8_t> betaValue{'b', 'e', 't', 'a'};

        for (const auto& test : cases) {
            const fs::path dir = fs::temp_directory_path() / ("akkaradb_compaction_crash_" + std::string{test.point});
            std::error_code ec;
            fs::remove_all(dir, ec);
            fs::create_directories(dir, ec);
            require(!ec, "failed to create compaction crash test directory");
            const int status = std::system(crashChildCommand(executable, "--compaction-crash-writer", test.point, dir).c_str());
            require(status == expectedCrashStatus(), "compaction crash writer did not terminate at the requested persistence boundary");

            auto options = persistentOptions(dir);
            options.sst.compactionMode = akkaradb::engine::sst::SSTCompactionMode::DISABLED;
            options.sst.compactThreads = 0;
            auto engine = akkaradb::engine::AkkEngine::open(options);
            const auto alpha = engine->get(bytes(alphaKey));
            const auto beta = engine->get(bytes(betaKey));
            require(alpha.has_value() && *alpha == alphaValue, "compaction crash recovery lost alpha");
            require(beta.has_value() && *beta == betaValue, "compaction crash recovery lost beta");
            require(engine->stats().currentSeq == 2, "compaction crash recovery reused a durable sequence");

            size_t l0Count = 0;
            size_t l1Count = 0;
            for (const auto& entry : fs::directory_iterator(dir / "sstable")) {
                require(entry.path().extension() != ".tmp", "compaction recovery did not remove temporary output");
                const std::string filename = entry.path().filename().string();
                if (filename.rfind("L0_", 0) == 0) { ++l0Count; }
                if (filename.rfind("L1_", 0) == 0) { ++l1Count; }
            }
            require(
                test.expectCompactedOutput ? l0Count == 0 && l1Count == 1 : l0Count == 2 && l1Count == 0,
                "compaction recovery retained the wrong manifest-selected SST set"
            );
            engine->close();
            fs::remove_all(dir, ec);
        }
    }

    void runRecoveryTest(const fs::path& executable) {
        const fs::path dir = fs::temp_directory_path() / "akkaradb_engine_recovery_smoke";
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir, ec);
        require(!ec, "failed to create recovery test directory");

        // std::system uses cmd.exe on Windows. Its /c parser requires an outer
        // quote pair when the executable path itself is quoted.
#ifdef _WIN32
        const std::string command = "\"\"" + executable.string() + "\" --crash-writer " + quoteArgument(dir) + "\"";
#else
        const std::string command = quoteArgument(executable) + " --crash-writer " + quoteArgument(dir);
#endif
        require(std::system(command.c_str()) == 0, "crash-writer subprocess failed");

        const std::vector<uint8_t> key{'c', 'r', 'a', 's', 'h', '-', 'k', 'e', 'y'};
        const std::vector<uint8_t> value{'c', 'r', 'a', 's', 'h', '-', 'v', 'a', 'l', 'u', 'e'};
        {
            auto engine = akkaradb::engine::AkkEngine::open(persistentOptions(dir));
            const auto recovered = engine->get(bytes(key));
            require(recovered.has_value() && *recovered == value, "WAL recovery did not restore crash-writer data");
            engine->forceFlush();
            engine->close();
        }

        // A stale temporary SST is expected after interruption before the rename.
        const fs::path sstDir = dir / "sstable";
        const fs::path staleTmp = sstDir / "L0_interrupted.aksst.tmp";
        std::ofstream tmp{staleTmp, std::ios::binary | std::ios::trunc};
        tmp << "partial SST";
        tmp.close();
        require(fs::exists(staleTmp), "failed to create stale SST temporary file");

        fs::path sealedSst;
        for (const auto& entry : fs::directory_iterator(sstDir)) {
            if (entry.path().extension() == ".aksst") {
                sealedSst = entry.path();
                break;
            }
        }
        require(!sealedSst.empty(), "flush did not create an SST");
        // This models an SST rename that completed before the corresponding
        // manifest seal was durable. Recovery must not treat it as live.
        fs::copy_file(sealedSst, sstDir / "L0_unsealed_after_crash.aksst", fs::copy_options::overwrite_existing, ec);
        require(!ec, "failed to create unsealed SST crash artifact");

        // Remove the WAL after a successful flush. The next open must use the
        // manifest-selected SST rather than a retained WAL record.
        fs::remove_all(dir / "wal", ec);
        require(!ec, "failed to remove test WAL after SST flush");

        {
            auto engine = akkaradb::engine::AkkEngine::open(persistentOptions(dir));
            const auto recovered = engine->get(bytes(key));
            require(recovered.has_value() && *recovered == value, "SST/manifest recovery did not restore flushed data");
            require(engine->stats().currentSeq == 1, "SST recovery did not restore the visible sequence");
            const std::vector<uint8_t> nextKey{'n', 'e', 'x', 't', '-', 'k', 'e', 'y'};
            const std::vector<uint8_t> nextValue{'n', 'e', 'x', 't', '-', 'v', 'a', 'l', 'u', 'e'};
            engine->put(bytes(nextKey), bytes(nextValue));
            require(engine->stats().currentSeq == 2, "writes after SST recovery reused a sequence number");
            const auto nextRecovered = engine->get(bytes(nextKey));
            require(nextRecovered.has_value() && *nextRecovered == nextValue, "post-recovery write was not visible");
            engine->close();
        }
        require(!fs::exists(staleTmp), "SST recovery did not remove stale temporary output");

        fs::remove_all(dir, ec);
    }

    void runManifestCheckpointSkipsCoveredWalRecoveryTest() {
        namespace wal = akkaradb::engine::wal;

        const fs::path dir = fs::temp_directory_path() / "akkaradb_manifest_checkpoint_wal_skip_smoke";
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir, ec);
        require(!ec, "failed to create manifest checkpoint WAL skip test directory");

        const std::vector<uint8_t> key{'m', 'a', 'n', 'i', 'f', 'e', 's', 't', '-', 'w', 'a', 'l', '-', 'k', 'e', 'y'};
        const std::vector<uint8_t> flushedValue{'f', 'l', 'u', 's', 'h', 'e', 'd'};
        const std::vector<uint8_t> staleWalValue{'s', 't', 'a', 'l', 'e', '-', 'w', 'a', 'l'};

        {
            auto engine = akkaradb::engine::AkkEngine::open(persistentOptions(dir));
            engine->put(bytes(key), bytes(flushedValue));
            engine->forceFlush();
            engine->close();
        }

        {
            wal::WalOptions wopts;
            wopts.walDir = dir / "wal";
            wopts.shardCount = 1;
            wopts.execution = wal::WalExecutionMode::INLINE;
            wopts.syncPolicy = wal::WalSyncPolicy::ALWAYS;
            auto writer = wal::WalWriter::create(wopts);
            writer->append(bytes(key), bytes(staleWalValue), 1, 0, 0, wal::WalAppendAck::SYNCED);
            writer->close();
        }

        {
            auto options = persistentOptions(dir);
            options.runtime.pruneWalOnFlush = false;
            auto engine = akkaradb::engine::AkkEngine::open(std::move(options));
            const auto recovered = engine->get(bytes(key));
            require(recovered.has_value() && *recovered == flushedValue, "manifest checkpoint did not suppress covered WAL replay");
            require(engine->stats().currentSeq == 1, "manifest checkpoint WAL skip restored the wrong sequence");
            engine->close();
        }

        fs::remove_all(dir, ec);
    }

    void runManifestCheckpointIntegrityRejectsMissingSstTest() {
        namespace manifest = akkaradb::engine::manifest;

        const fs::path dir = fs::temp_directory_path() / "akkaradb_manifest_checkpoint_integrity_smoke";
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir, ec);
        require(!ec, "failed to create manifest checkpoint integrity test directory");

        {
            auto mf = manifest::Manifest::create(dir / "manifest.akmf", false);
            mf->checkpoint(std::optional<std::string>{"corrupt"}, std::nullopt, 10);
            mf->close();
        }

        bool rejected = false;
        try {
            (void)akkaradb::engine::AkkEngine::open(persistentOptions(dir));
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        require(rejected, "engine accepted a manifest checkpoint beyond recovered SST state");

        fs::remove_all(dir, ec);
    }

    void runManifestStatsExposeRecoveryMetadataTest() {
        const fs::path dir = fs::temp_directory_path() / "akkaradb_manifest_stats_smoke";
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir, ec);
        require(!ec, "failed to create manifest stats test directory");

        const std::vector<uint8_t> key{'m', 'a', 'n', 'i', 'f', 'e', 's', 't', '-', 's', 't', 'a', 't', 's'};
        const std::vector<uint8_t> value{'v', 'a', 'l', 'u', 'e'};
        {
            auto engine = akkaradb::engine::AkkEngine::open(persistentOptions(dir));
            engine->put(bytes(key), bytes(value));
            engine->forceFlush();
            const auto stats = engine->stats();
            require(stats.manifest.enabled, "manifest stats did not report enabled manifest");
            require(stats.manifest.hasCheckpoint, "manifest stats did not expose checkpoint presence");
            require(stats.manifest.lastCheckpointSeq == 1, "manifest stats exposed the wrong checkpoint sequence");
            require(stats.manifest.liveSstCount == 1, "manifest stats exposed the wrong live SST count");
            require(stats.manifest.sstSealCount == 1, "manifest stats exposed the wrong SST seal count");
            engine->close();
        }

        fs::remove_all(dir, ec);
    }

    void runManifestStatsExposeBlobLifecycleTest() {
        const fs::path dir = fs::temp_directory_path() / "akkaradb_manifest_blob_lifecycle_smoke";
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir, ec);
        require(!ec, "failed to create manifest blob lifecycle test directory");

        auto options = persistentOptions(dir);
        options.components.blobEnabled = true;
        options.blob.thresholdBytes = 8;

        const std::string key = "blob-manifest-key";
        const std::vector<uint8_t> value(64, 'b');

        auto engine = akkaradb::engine::AkkEngine::open(options);
        engine->put(bytes(key), bytes(value));
        engine->forceFlush();
        auto stats = engine->stats();
        require(stats.manifest.blobPutCount == 1, "manifest stats must count blob put records");
        require(stats.manifest.liveBlobCount == 1, "manifest stats must expose live blob count");
        require(stats.manifest.deletedBlobCount == 0, "manifest stats must not mark live blob as deleted");
        require(stats.manifest.sstBlobRefsComplete, "manifest stats must mark flushed SST blob refs complete");
        require(stats.manifest.sstReferencedBlobCount == 1, "manifest stats must expose SST-referenced blob count");

        engine->remove(bytes(key));
        engine->runBlobGc();
        require(
            waitUntil([&] {
                const auto current = engine->stats();
                return current.blob.blobsDeleted == 1 && current.manifest.blobDeleteCount == 1 &&
                    current.manifest.liveBlobCount == 0 && current.manifest.deletedBlobCount == 1;
            }),
            "manifest stats must reflect blob GC delete records"
        );
        engine->close();

        auto reopened = akkaradb::engine::AkkEngine::open(options);
        stats = reopened->stats();
        require(stats.manifest.blobPutCount == 1, "manifest replay must retain blob put records");
        require(stats.manifest.blobDeleteCount == 1, "manifest replay must retain blob delete records");
        require(stats.manifest.liveBlobCount == 0, "manifest replay must retain live blob count");
        require(stats.manifest.deletedBlobCount == 1, "manifest replay must retain deleted blob count");
        require(stats.manifest.sstBlobRefsComplete, "manifest replay must retain SST blob ref completeness");
        require(stats.manifest.sstReferencedBlobCount == 0, "manifest replay must account for tombstones in SST blob refs");
        reopened->close();

        fs::remove_all(dir, ec);
    }

    void runParallelWalMatrixRecoveryTest(const fs::path& executable) {
        const fs::path dir = fs::temp_directory_path() / "akkaradb_parallel_wal_matrix_recovery_smoke";
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir, ec);
        require(!ec, "failed to create parallel WAL matrix recovery test directory");

#ifdef _WIN32
        const std::string command = "\"\"" + executable.string() + "\" --parallel-wal-crash-writer " + quoteArgument(dir) + "\"";
#else
        const std::string command = quoteArgument(executable) + " --parallel-wal-crash-writer " + quoteArgument(dir);
#endif
        require(std::system(command.c_str()) == 0, "parallel WAL crash-writer subprocess failed");

        const MatrixData data;
        {
            auto engine = akkaradb::engine::AkkEngine::open(parallelWalOptions(dir));
            requireMatrixRecords(*engine, data);
            engine->forceFlush();
            engine->close();
        }

        fs::remove_all(dir / "wal", ec);
        require(!ec, "failed to remove parallel WAL matrix test WAL after flush");

        {
            auto engine = akkaradb::engine::AkkEngine::open(parallelWalOptions(dir));
            requireMatrixRecords(*engine, data);
            const std::vector<uint8_t> nextKey{'m', 'a', 't', 'r', 'i', 'x', '-', 'n', 'e', 'x', 't'};
            const std::vector<uint8_t> nextValue{'n', 'e', 'x', 't'};
            engine->put(bytes(nextKey), bytes(nextValue));
            require(engine->stats().currentSeq == 6, "post-SST matrix recovery write reused a sequence number");
            engine->close();
        }

        fs::remove_all(dir, ec);
    }

    void runParallelWalSstCorrectnessTest() {
        using EngineOptions = akkaradb::engine::AkkEngineOptions;
        using WalExecutionMode = akkaradb::engine::wal::WalExecutionMode;

        struct Case {
            const char* name;
            EngineOptions::WriteVisibilityMode visibility;
            WalExecutionMode walExecution;
        };
        constexpr std::array cases{
            Case{"commit-async", EngineOptions::WriteVisibilityMode::COMMIT_ORDER, WalExecutionMode::ASYNC},
            Case{"applied-async", EngineOptions::WriteVisibilityMode::APPLIED, WalExecutionMode::ASYNC},
            Case{"commit-inline", EngineOptions::WriteVisibilityMode::COMMIT_ORDER, WalExecutionMode::INLINE},
            Case{"applied-inline", EngineOptions::WriteVisibilityMode::APPLIED, WalExecutionMode::INLINE},
        };

        constexpr size_t writerCount = 4;
        constexpr size_t operationsPerWriter = 32;
        constexpr uint64_t writesPerOperation = 5; // put + two batch entries + put/remove tombstone

        for (const auto& test : cases) {
            const fs::path dir = fs::temp_directory_path() / ("akkaradb_parallel_wal_sst_" + std::string{test.name});
            std::error_code ec;
            fs::remove_all(dir, ec);
            fs::create_directories(dir, ec);
            require(!ec, "failed to create parallel WAL/SST correctness test directory");

            auto options = parallelWalOptions(dir, test.visibility, test.walExecution);
            auto engine = akkaradb::engine::AkkEngine::open(options);
            const std::string stableKey{"parallel-stable-key"};
            const std::string stableValue{"parallel-stable-value"};
            engine->put(bytes(stableKey), bytes(stableValue));
            engine->forceFlush();
            const uint64_t baselineSeq = engine->stats().currentSeq;

            std::atomic<size_t> writersReady{0};
            std::atomic<size_t> writersRemaining{writerCount};
            std::atomic<bool> start{false};
            std::atomic<bool> readerRunning{true};
            std::atomic<bool> flushActive{false};
            std::atomic<uint64_t> readsDuringFlush{0};
            std::mutex failureMutex;
            std::exception_ptr failure;
            const auto recordFailure = [&](std::exception_ptr error) {
                std::lock_guard lock{failureMutex};
                if (!failure) { failure = std::move(error); }
            };

            std::thread reader([&] {
                try {
                    while (!start.load(std::memory_order_acquire)) { std::this_thread::yield(); }
                    while (readerRunning.load(std::memory_order_acquire)) {
                        const auto value = engine->get(bytes(stableKey));
                        require(value.has_value() && *value == std::vector<uint8_t>(stableValue.begin(), stableValue.end()),
                                "read during parallel SST flush lost a stable value");
                        if (flushActive.load(std::memory_order_acquire)) {
                            readsDuringFlush.fetch_add(1, std::memory_order_relaxed);
                        }
                    }
                }
                catch (...) { recordFailure(std::current_exception()); }
            });

            std::thread flusher([&] {
                try {
                    while (!start.load(std::memory_order_acquire)) { std::this_thread::yield(); }
                    size_t flushes = 0;
                    while (writersRemaining.load(std::memory_order_acquire) != 0 || flushes < 16) {
                        flushActive.store(true, std::memory_order_release);
                        std::this_thread::yield();
                        engine->forceFlush();
                        flushActive.store(false, std::memory_order_release);
                        ++flushes;
                        std::this_thread::yield();
                    }
                }
                catch (...) { recordFailure(std::current_exception()); }
            });

            std::vector<std::thread> writers;
            writers.reserve(writerCount);
            for (size_t writer = 0; writer < writerCount; ++writer) {
                writers.emplace_back([&, writer] {
                    try {
                        writersReady.fetch_add(1, std::memory_order_release);
                        while (!start.load(std::memory_order_acquire)) { std::this_thread::yield(); }
                        for (size_t operation = 0; operation < operationsPerWriter; ++operation) {
                            const std::string base = "parallel/" + std::string{test.name} + "/" + std::to_string(writer) + "/" + std::to_string(operation);
                            const std::string putKey = base + "/put";
                            const std::string putValue = "value/" + std::to_string(writer) + "/" + std::to_string(operation);
                            const std::string batchFirstKey = base + "/batch-first";
                            const std::string batchSecondKey = base + "/batch-second";
                            const std::string batchFirstValue = "batch-first/" + std::to_string(operation);
                            const std::string batchSecondValue = "batch-second/" + std::to_string(operation);
                            const std::string removedKey = base + "/removed";

                            engine->put(bytes(putKey), bytes(putValue));
                            const std::array batch{
                                akkaradb::engine::AkkEngine::BatchPutEntry{bytes(batchFirstKey), bytes(batchFirstValue)},
                                akkaradb::engine::AkkEngine::BatchPutEntry{bytes(batchSecondKey), bytes(batchSecondValue)},
                            };
                            engine->putBatch(batch);
                            engine->put(bytes(removedKey), bytes("removed"));
                            engine->remove(bytes(removedKey));
                            if ((operation & 3U) == 0) { std::this_thread::yield(); }
                        }
                    }
                    catch (...) { recordFailure(std::current_exception()); }
                    writersRemaining.fetch_sub(1, std::memory_order_release);
                });
            }

            while (writersReady.load(std::memory_order_acquire) != writerCount) { std::this_thread::yield(); }
            start.store(true, std::memory_order_release);
            for (auto& writer : writers) { writer.join(); }
            flusher.join();
            readerRunning.store(false, std::memory_order_release);
            reader.join();
            if (failure) { std::rethrow_exception(failure); }

            require(readsDuringFlush.load(std::memory_order_relaxed) > 0, "reader did not run while SST flush was active");
            engine->forceFlush();
            engine->forceSync();
            const uint64_t expectedSeq = baselineSeq + writerCount * operationsPerWriter * writesPerOperation;
            const uint64_t currentSeq = engine->stats().currentSeq;
            require(
                currentSeq == expectedSeq,
                (std::string{"parallel writes did not publish every reserved sequence in "} + test.name + " (expected " + std::to_string(expectedSeq) +
                 ", got " + std::to_string(currentSeq) + ")").c_str()
            );

            const auto verify = [&](akkaradb::engine::AkkEngine& target) {
                const auto requireValue = [&](std::string_view key, std::string_view expected, const char* message) {
                    const auto actual = target.get(bytes(key));
                    require(actual.has_value() && *actual == std::vector<uint8_t>(expected.begin(), expected.end()), message);
                };
                const auto stable = target.get(bytes(stableKey));
                require(stable.has_value() && *stable == std::vector<uint8_t>(stableValue.begin(), stableValue.end()),
                        "parallel WAL/SST recovery lost the stable value");
                for (size_t writer = 0; writer < writerCount; ++writer) {
                    for (size_t operation = 0; operation < operationsPerWriter; ++operation) {
                        const std::string base = "parallel/" + std::string{test.name} + "/" + std::to_string(writer) + "/" + std::to_string(operation);
                        const std::string putKey = base + "/put";
                        const std::string putValue = "value/" + std::to_string(writer) + "/" + std::to_string(operation);
                        const std::string batchFirstKey = base + "/batch-first";
                        const std::string batchSecondKey = base + "/batch-second";
                        const std::string batchFirstValue = "batch-first/" + std::to_string(operation);
                        const std::string batchSecondValue = "batch-second/" + std::to_string(operation);
                        requireValue(putKey, putValue, "parallel put was lost or corrupted");
                        requireValue(batchFirstKey, batchFirstValue, "parallel batch first entry was lost or corrupted");
                        requireValue(batchSecondKey, batchSecondValue, "parallel batch second entry was lost or corrupted");
                        require(!target.get(bytes(base + "/removed")).has_value(), "parallel remove tombstone was lost");
                    }
                }
                require(target.stats().currentSeq == expectedSeq, "parallel WAL/SST recovery reused a sequence");
            };

            verify(*engine);
            engine->close();
            auto reopened = akkaradb::engine::AkkEngine::open(options);
            verify(*reopened);
            reopened->close();
            fs::remove_all(dir, ec);
        }
    }
}

int main(int argc, char** argv) {
    akkaradb::test::installMsvcTestErrorHandlers();
    try {
        if (argc == 3 && std::string{argv[1]} == "--crash-writer") { return runCrashWriter(fs::path{argv[2]}); }
        if (argc == 3 && std::string{argv[1]} == "--parallel-wal-crash-writer") { return runParallelWalCrashWriter(fs::path{argv[2]}); }
        if (argc == 4 && std::string{argv[1]} == "--flush-crash-writer") { return runFlushCrashWriter(argv[2], fs::path{argv[3]}); }
        if (argc == 4 && std::string{argv[1]} == "--compaction-crash-writer") { return runCompactionCrashWriter(argv[2], fs::path{argv[3]}); }
        if (argc != 1) { throw std::invalid_argument("unexpected command line"); }
        runRecoveryTest(fs::absolute(fs::path{argv[0]}));
        runManifestCheckpointSkipsCoveredWalRecoveryTest();
        runManifestCheckpointIntegrityRejectsMissingSstTest();
        runManifestStatsExposeRecoveryMetadataTest();
        runManifestStatsExposeBlobLifecycleTest();
        runParallelWalMatrixRecoveryTest(fs::absolute(fs::path{argv[0]}));
        runParallelWalSstCorrectnessTest();
        runFlushBoundaryCrashRecoveryTests(fs::absolute(fs::path{argv[0]}));
        runCompactionCrashRecoveryTests(fs::absolute(fs::path{argv[0]}));
        return 0;
    }
    catch (const std::exception& ex) {
        std::fprintf(stderr, "engine recovery smoke failed: %s\n", ex.what());
        return 1;
    }
}

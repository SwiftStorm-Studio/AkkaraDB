/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "TestErrorHandlers.hpp"

#include "akk/engine/AkkEngine.hpp"
#include "akk/engine/vlog/VersionLog.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <exception>
#include <filesystem>
#include <limits>
#include <mutex>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {
    namespace fs = std::filesystem;
    namespace vlog = akkaradb::engine::vlog;

    void require(bool condition, const char* message) {
        if (!condition) { throw std::runtime_error(message); }
    }

    [[nodiscard]] std::span<const uint8_t> bytes(std::string_view value) noexcept {
        return {reinterpret_cast<const uint8_t*>(value.data()), value.size()};
    }

    class TempDir {
        public:
            TempDir() {
                const auto id = std::chrono::steady_clock::now().time_since_epoch().count();
                path_ = fs::temp_directory_path() / ("akkaradb-vlog-admission-" + std::to_string(id));
                fs::create_directories(path_);
            }

            ~TempDir() {
                std::error_code ignored;
                fs::remove_all(path_, ignored);
            }

            [[nodiscard]] const fs::path& path() const noexcept { return path_; }

        private:
            fs::path path_;
    };

    [[nodiscard]] vlog::VersionLogOptions logOptions(vlog::VLogReadVisibilityMode visibility) {
        vlog::VersionLogOptions options;
        options.syncMode = vlog::VLogSyncMode::SYNC;
        options.readVisibility = visibility;
        return options;
    }

    void verifyReadVisibility(const fs::path& dir) {
        constexpr std::string_view key{"visibility-key"};
        constexpr std::string_view value{"value"};

        {
            auto log = vlog::VersionLog::create(dir / "commit.akvlog", logOptions(vlog::VLogReadVisibilityMode::COMMIT_ORDER));
            log->appendDeferred(bytes(key), 1, 0, 0, 0, bytes(value));
            require(!log->getAt(bytes(key), 1).has_value(), "COMMIT_ORDER must hide a deferred append");
            require(log->history(bytes(key)).empty(), "COMMIT_ORDER history must hide a deferred append");
            log->markCommitted(1);
            const auto observed = log->getAt(bytes(key), 1);
            require(observed.has_value() && observed->value == std::vector<uint8_t>{value.begin(), value.end()}, "COMMIT_ORDER must reveal a committed append");
            log->close();
        }

        {
            auto log = vlog::VersionLog::create(dir / "applied.akvlog", logOptions(vlog::VLogReadVisibilityMode::APPLIED));
            log->appendDeferred(bytes(key), 1, 0, 0, 0, bytes(value));
            const auto observed = log->getAt(bytes(key), 1);
            require(observed.has_value() && observed->value == std::vector<uint8_t>{value.begin(), value.end()}, "APPLIED must reveal a deferred append");
            log->close();
        }
    }

    void verifyBackgroundRecovery(const fs::path& dir) {
        constexpr std::string_view key{"background-key"};
        constexpr std::string_view value{"value"};
        constexpr uint64_t entryCount = 128;
        const auto path = dir / "background.akvlog";

        {
            auto options = logOptions(vlog::VLogReadVisibilityMode::COMMIT_ORDER);
            options.syncMode = vlog::VLogSyncMode::ASYNC;
            auto log = vlog::VersionLog::create(path, std::move(options));
            for (uint64_t seq = 1; seq <= entryCount; ++seq) {
                log->append(bytes(key), seq, 0, 0, 0, bytes(value));
            }
            log->close();
        }

        auto options = logOptions(vlog::VLogReadVisibilityMode::COMMIT_ORDER);
        options.recoveryMode = vlog::VLogRecoveryMode::BACKGROUND;
        auto log = vlog::VersionLog::create(path, std::move(options));
        // getAt waits for background validation and scans the persisted history on demand.
        const auto observed = log->getAt(bytes(key), entryCount);
        require(observed.has_value() && observed->seq == entryCount, "background recovery must expose persisted history after waiting");
        require(log->history(bytes(key)).size() == entryCount, "on-demand history must retain every persisted version");
        log->close();
    }

    void verifyZstdCompression(const fs::path& dir) {
        constexpr std::string_view key{"compression-key"};
        constexpr std::string_view plainValue{"plain"};
        const std::string repeatedValue(16 * 1024, 'z');
        const auto path = dir / "compression.akvlog";

        {
            auto options = logOptions(vlog::VLogReadVisibilityMode::COMMIT_ORDER);
            require(options.codec == vlog::VLogCodec::NONE, "VersionLog compression must default to NONE");
            auto log = vlog::VersionLog::create(path, std::move(options));
            log->append(bytes(key), 1, 0, 0, 0, bytes(plainValue));
            log->close();
        }

        {
            auto options = logOptions(vlog::VLogReadVisibilityMode::COMMIT_ORDER);
            options.codec = vlog::VLogCodec::ZSTD;
            options.zstdCompressionLevel = 3;
            auto log = vlog::VersionLog::create(path, std::move(options));
            const auto snapshot = log->snapshot();
            require(snapshot.codec == static_cast<uint8_t>(vlog::VLogCodec::ZSTD) && snapshot.zstdCompressionLevel == 3,
                    "VersionLog must expose the configured Zstd codec and level");
            log->append(bytes(key), 2, 0, 0, 0, bytes(repeatedValue));
            log->close();
        }

        require(!fs::exists(dir / "compression-seg-1.akvlog"), "enabling Zstd must retain the active v1 segment");
        require(fs::file_size(path) < repeatedValue.size(), "VersionLog must persist compressible values in Zstd form");

        {
            auto options = logOptions(vlog::VLogReadVisibilityMode::COMMIT_ORDER);
            options.codec = vlog::VLogCodec::ZSTD;
            options.zstdCompressionLevel = 3;
            auto log = vlog::VersionLog::create(path, std::move(options));
            const auto original = log->getAt(bytes(key), 1);
            const auto compressed = log->getAt(bytes(key), 2);
            require(original.has_value() && original->value == std::vector<uint8_t>{plainValue.begin(), plainValue.end()},
                    "VersionLog must retain readable legacy records after enabling Zstd");
            require(compressed.has_value() && compressed->flags == 0 &&
                        compressed->value == std::vector<uint8_t>{repeatedValue.begin(), repeatedValue.end()},
                    "VersionLog must transparently decompress persisted values");
            log->close();
        }

        const auto invalidPath = dir / "invalid-zstd-level.akvlog";
        auto invalid = logOptions(vlog::VLogReadVisibilityMode::COMMIT_ORDER);
        invalid.codec = vlog::VLogCodec::ZSTD;
        invalid.zstdCompressionLevel = std::numeric_limits<int>::max();
        bool rejected = false;
        try { (void)vlog::VersionLog::create(invalidPath, std::move(invalid)); }
        catch (const std::invalid_argument&) { rejected = true; }
        require(rejected, "VersionLog must reject an unsupported Zstd level at startup");
    }

    void verifySegmentation(const fs::path& dir) {
        constexpr std::string_view key{"segment-key"};
        constexpr std::string_view value{"value"};
        constexpr uint64_t entryCount = 12;
        const auto path = dir / "segments.akvlog";

        {
            auto options = logOptions(vlog::VLogReadVisibilityMode::COMMIT_ORDER);
            options.segmentBytes = 256;
            auto log = vlog::VersionLog::create(path, std::move(options));
            log->append(bytes(key), 0, 0, 0, 0, bytes(value));
            for (uint64_t seq = 1; seq <= entryCount; ++seq) {
                log->append(bytes(key), seq, 0, 0, 0, bytes(value));
            }
            const auto snapshot = log->snapshot();
            require(snapshot.segmentCount > 1, "VersionLog must rotate at the configured segment byte threshold");
            log->close();
        }

        require(fs::exists(dir / "segments-seg-1.akvlog"), "VersionLog must create numbered sibling segments without changing the extension");
        require(fs::exists(dir / "segments.akvidx"), "VersionLog must persist a sidecar index for segment zero");
        require(fs::exists(dir / "segments-seg-1.akvidx"), "VersionLog must persist a sidecar index for closed segments");
        auto options = logOptions(vlog::VLogReadVisibilityMode::COMMIT_ORDER);
        options.segmentBytes = 256;
        options.recoveryMode = vlog::VLogRecoveryMode::BACKGROUND;
        auto log = vlog::VersionLog::create(path, std::move(options));
        const auto zero = log->getAt(bytes(key), 0);
        require(zero.has_value() && zero->seq == 0, "segment index must retain a zero-sequence version");
        const auto observed = log->getAt(bytes(key), 6);
        require(observed.has_value() && observed->seq == 6, "segment index must retain the version range needed by getAt");
        require(log->history(bytes(key)).size() == entryCount + 1, "history must return every indexed segment entry");
        require(log->snapshot().segmentCount > 1, "background recovery must rebuild the segment index");
        const auto baseIndexPath = dir / "segments.akvidx";
        #ifdef _WIN32
        FILE* corruptedIndex = _wfopen(baseIndexPath.wstring().c_str(), L"r+b");
        #else
        FILE* corruptedIndex = fopen(baseIndexPath.string().c_str(), "r+b");
        #endif
        require(corruptedIndex != nullptr, "VersionLog sidecar index must be writable for corruption fallback verification");
        require(fseek(corruptedIndex, -1, SEEK_END) == 0, "VersionLog sidecar index must support payload corruption verification");
        const int lastByte = fgetc(corruptedIndex);
        require(lastByte != EOF && fseek(corruptedIndex, -1, SEEK_CUR) == 0 && fputc(lastByte ^ 0x01, corruptedIndex) != EOF,
                "VersionLog sidecar index corruption setup must succeed");
        fclose(corruptedIndex);
        fs::remove(dir / "segments-seg-1.akvidx");
        require(log->history(bytes(key)).size() == entryCount + 1,
                "missing or corrupt derived segment indexes must fall back to the authoritative VLog segment");
        log->close();
    }

    void verifySingleFileIndexFallback(const fs::path& dir) {
        constexpr std::string_view key{"single-file-index-key"};
        constexpr std::string_view value{"value"};
        const auto path = dir / "single-file.akvlog";

        {
            auto options = logOptions(vlog::VLogReadVisibilityMode::COMMIT_ORDER);
            options.segmentBytes = 0;
            auto log = vlog::VersionLog::create(path, std::move(options));
            log->append(bytes(key), 1, 0, 0, 0, bytes(value));
            log->append(bytes(key), 2, 0, 0, 0, bytes(value));
            require(log->history(bytes(key)).size() == 2,
                    "a single-file VLog must scan safely instead of retaining an unbounded active index");
            log->close();
        }

        auto options = logOptions(vlog::VLogReadVisibilityMode::COMMIT_ORDER);
        options.segmentBytes = 0;
        auto log = vlog::VersionLog::create(path, std::move(options));
        require(log->history(bytes(key)).size() == 2, "recovery must regenerate the single-file index sidecar");
        log->append(bytes(key), 3, 0, 0, 0, bytes(value));
        require(log->history(bytes(key)).size() == 3,
                "a stale single-file sidecar must fall back to the authoritative VLog data");
        log->close();
    }

    void verifyRetention(const fs::path& dir) {
        constexpr std::string_view key{"retention-key"};
        constexpr std::string_view value{"value"};
        const auto sequencePath = dir / "retention-sequence.akvlog";

        {
            auto options = logOptions(vlog::VLogReadVisibilityMode::COMMIT_ORDER);
            options.segmentBytes = 150;
            auto log = vlog::VersionLog::create(sequencePath, std::move(options));
            for (uint64_t seq = 1; seq <= 12; ++seq) {
                log->append(bytes(key), seq, 0, 0, 0, bytes(value));
            }
            log->close();
        }

        {
            auto options = logOptions(vlog::VLogReadVisibilityMode::COMMIT_ORDER);
            options.segmentBytes = 150;
            options.retentionMinCommitSeq = 7;
            auto log = vlog::VersionLog::create(sequencePath, std::move(options));
            const auto snapshot = log->snapshot();
            require(snapshot.retentionMinCommitSeq == 7, "VersionLog must expose the configured commit-sequence retention boundary");
            require(!fs::exists(sequencePath) && !fs::exists(dir / "retention-sequence-seg-1.akvlog"),
                    "commit-sequence retention must remove expired closed segments");
            require(fs::exists(dir / "retention-sequence-seg-3.akvlog"),
                    "commit-sequence retention must retain the segment containing the cutoff");
            require(!log->getAt(bytes(key), 6).has_value(), "expired history must not be returned before the retained sequence boundary");
            const auto retained = log->getAt(bytes(key), 7);
            require(retained.has_value() && retained->seq == 7, "retention must preserve the cutoff sequence and newer history");
            require(log->history(bytes(key)).size() == 6, "retention must expose only retained history entries");
            log->close();
        }

        const auto compactedPath = dir / "retention-base.akvlog";
        constexpr std::string_view carriedKey{"carried-key"};
        constexpr std::string_view driverKey{"retention-driver"};
        constexpr std::string_view carriedValue{"carried-value"};
        {
            auto options = logOptions(vlog::VLogReadVisibilityMode::COMMIT_ORDER);
            options.segmentBytes = 150;
            auto log = vlog::VersionLog::create(compactedPath, std::move(options));
            log->append(bytes(carriedKey), 1, 0, 0, 0, bytes(carriedValue));
            for (uint64_t seq = 2; seq <= 10; ++seq) {
                log->append(bytes(driverKey), seq, 0, 0, 0, bytes(value));
            }
            log->close();
        }

        {
            auto options = logOptions(vlog::VLogReadVisibilityMode::COMMIT_ORDER);
            options.segmentBytes = 150;
            options.retentionMinCommitSeq = 7;
            auto log = vlog::VersionLog::create(compactedPath, std::move(options));
            require(!log->getAt(bytes(carriedKey), 6).has_value(),
                    "retention compaction must not recreate history before the configured sequence boundary");
            const auto carried = log->getAt(bytes(carriedKey), 7);
            require(carried.has_value() && carried->seq == 7 &&
                        carried->value == std::vector<uint8_t>{carriedValue.begin(), carriedValue.end()} &&
                        (carried->flags & vlog::VLOG_FLAG_RETENTION_BASE) != 0,
                    "retention compaction must preserve the carried state at the sequence boundary");
            const auto carriedHistory = log->history(bytes(carriedKey));
            require(carriedHistory.size() == 1 && carriedHistory.front().seq == 7 &&
                        (carriedHistory.front().flags & vlog::VLOG_FLAG_RETENTION_BASE) != 0,
                    "history must expose the synthetic retention base entry");
            log->close();
        }

        const auto agePath = dir / "retention-age.akvlog";
        {
            auto options = logOptions(vlog::VLogReadVisibilityMode::COMMIT_ORDER);
            options.segmentBytes = 150;
            auto log = vlog::VersionLog::create(agePath, std::move(options));
            for (uint64_t seq = 1; seq <= 4; ++seq) {
                log->append(bytes(key), seq, 0, 0, 0, bytes(value));
            }
            log->close();
        }
        fs::last_write_time(agePath, fs::file_time_type::clock::now() - std::chrono::hours{48});

        {
            auto options = logOptions(vlog::VLogReadVisibilityMode::COMMIT_ORDER);
            options.segmentBytes = 150;
            options.retentionDays = 1;
            auto log = vlog::VersionLog::create(agePath, std::move(options));
            require(!fs::exists(agePath), "age retention must remove a closed segment older than the configured number of days");
            require(fs::exists(dir / "retention-age-seg-1.akvlog"), "age retention must retain newer closed segments");
            require(log->history(bytes(key)).size() == 2, "age retention must retain only entries from non-expired segments");
            log->close();
        }
    }

    void verifyConcurrentRetentionCompaction(const fs::path& dir) {
        constexpr std::string_view carriedKey{"retention-concurrent-carry"};
        constexpr std::string_view driverKey{"retention-concurrent-driver"};
        constexpr std::string_view value{"value"};
        const auto path = dir / "retention-concurrent.akvlog";

        auto options = logOptions(vlog::VLogReadVisibilityMode::COMMIT_ORDER);
        options.segmentBytes = 150;
        options.retentionDays = 1;
        auto log = vlog::VersionLog::create(path, std::move(options));
        log->append(bytes(carriedKey), 1, 0, 0, 0, bytes(value));
        for (uint64_t seq = 2; seq <= 16; ++seq) {
            log->append(bytes(driverKey), seq, 0, 0, 0, bytes(value));
        }
        fs::last_write_time(path, fs::file_time_type::clock::now() - std::chrono::hours{48});

        std::atomic<bool> start{false};
        std::exception_ptr failure;
        std::mutex failureMu;
        std::thread reader{[&] {
            while (!start.load(std::memory_order_acquire)) { std::this_thread::yield(); }
            try {
                for (uint32_t iteration = 0; iteration < 256; ++iteration) {
                    (void)log->history(bytes(driverKey));
                    (void)log->getAt(bytes(carriedKey), 64);
                }
            }
            catch (...) {
                std::lock_guard lock{failureMu};
                failure = std::current_exception();
            }
        }};

        start.store(true, std::memory_order_release);
        for (uint64_t seq = 17; seq <= 32; ++seq) {
            log->append(bytes(driverKey), seq, 0, 0, 0, bytes(value));
        }
        reader.join();
        if (failure) { std::rethrow_exception(failure); }
        const auto carried = log->getAt(bytes(carriedKey), 32);
        require(carried.has_value() && carried->value == std::vector<uint8_t>{value.begin(), value.end()},
                "retention compaction must preserve carried state while readers and writers run");
        log->close();
    }

    void verifyConcurrentReaders(const fs::path& dir) {
        constexpr std::string_view readKey{"read-key"};
        constexpr std::string_view writeKey{"write-key"};
        constexpr std::string_view value{"value"};
        constexpr uint64_t seedEntries = 256;
        constexpr uint32_t readerCount = 8;
        constexpr uint32_t readerIterations = 64;
        const auto path = dir / "readers.akvlog";

        auto options = logOptions(vlog::VLogReadVisibilityMode::COMMIT_ORDER);
        options.syncMode = vlog::VLogSyncMode::ASYNC;
        options.writeAdmission = vlog::VLogWriteAdmissionMode::PARALLEL;
        auto log = vlog::VersionLog::create(path, std::move(options));
        for (uint64_t seq = 1; seq <= seedEntries; ++seq) {
            log->append(bytes(readKey), seq, 0, 0, 0, bytes(value));
        }

        std::atomic<uint32_t> ready{0};
        std::atomic<bool> start{false};
        std::exception_ptr failure;
        std::mutex failureMu;
        const auto recordFailure = [&](std::exception_ptr error) {
            std::lock_guard lock{failureMu};
            if (!failure) { failure = std::move(error); }
        };

        std::vector<std::thread> readers;
        readers.reserve(readerCount);
        for (uint32_t reader = 0; reader < readerCount; ++reader) {
            readers.emplace_back([&] {
                ready.fetch_add(1, std::memory_order_release);
                while (!start.load(std::memory_order_acquire)) { std::this_thread::yield(); }
                try {
                    for (uint32_t iteration = 0; iteration < readerIterations; ++iteration) {
                        const auto history = log->history(bytes(readKey));
                        require(history.size() == seedEntries, "concurrent readers must retain the full key history");
                        const auto observed = log->getAt(bytes(readKey), seedEntries);
                        require(observed.has_value() && observed->seq == seedEntries, "concurrent readers must observe the requested version");
                    }
                }
                catch (...) { recordFailure(std::current_exception()); }
            });
        }
        std::thread writer{[&] {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) { std::this_thread::yield(); }
            try {
                for (uint64_t seq = seedEntries + 1; seq <= seedEntries * 2; ++seq) {
                    log->append(bytes(writeKey), seq, 0, 0, 0, bytes(value));
                }
            }
            catch (...) { recordFailure(std::current_exception()); }
        }};

        while (ready.load(std::memory_order_acquire) != readerCount + 1) { std::this_thread::yield(); }
        start.store(true, std::memory_order_release);
        for (auto& reader : readers) { reader.join(); }
        writer.join();
        if (failure) { std::rethrow_exception(failure); }
        require(log->history(bytes(writeKey)).size() == seedEntries, "concurrent writer must publish every version");
        log->close();
    }

    void verifySerialAppendGate(const fs::path& dir) {
        constexpr std::string_view key{"serial-append-gate"};
        constexpr std::string_view value{"value"};
        const auto path = dir / "serial-append-gate.akvlog";
        auto options = logOptions(vlog::VLogReadVisibilityMode::COMMIT_ORDER);
        options.syncMode = vlog::VLogSyncMode::ASYNC;
        options.serialAppendMode = vlog::VLogSerialAppendMode::WAIT_PREVIOUS_APPEND;
        options.asyncMaxPendingBytes = 1;
        auto log = vlog::VersionLog::create(path, std::move(options));
        log->append(bytes(key), 1, 0, 0, 0, bytes(value));
        log->append(bytes(key), 2, 0, 0, 0, bytes(value));
        const auto history = log->history(bytes(key));
        require(history.size() == 2 && history.back().seq == 2,
                "serial append gate must let the current put commit while gating the next VLog submission");
        log->close();
    }

    void verifyParallelPersistence(const fs::path& dir) {
        constexpr uint32_t writerCount = 8;
        constexpr uint32_t entriesPerWriter = 24;
        const auto path = dir / "true-parallel.akvlog";
        {
            auto options = logOptions(vlog::VLogReadVisibilityMode::COMMIT_ORDER);
            options.syncMode = vlog::VLogSyncMode::ASYNC;
            options.writeAdmission = vlog::VLogWriteAdmissionMode::PARALLEL;
            options.parallelWriteLanes = 4;
            options.parallelPendingLimitScope = vlog::VLogParallelPendingLimitScope::GLOBAL;
            auto log = vlog::VersionLog::create(path, std::move(options));

            std::vector<std::thread> writers;
            writers.reserve(writerCount);
            for (uint32_t writer = 0; writer < writerCount; ++writer) {
                writers.emplace_back([&, writer] {
                    const std::string key = "true-parallel-key-" + std::to_string(writer);
                    const std::string value = "true-parallel-value-" + std::to_string(writer);
                    const uint64_t firstSeq = static_cast<uint64_t>(writer) * entriesPerWriter + 1u;
                    for (uint32_t entry = 0; entry < entriesPerWriter; ++entry) {
                        log->append(bytes(key), firstSeq + entry, 0, 0, 0, bytes(value));
                    }
                });
            }
            for (auto& writer : writers) { writer.join(); }
            log->close();
        }

        require(fs::exists(dir / "true-parallel-seg-1.akvtail"), "parallel VLog must persist a durable tail for each active lane");
        require(fs::exists(dir / "true-parallel-seg-1.akvidx"), "parallel VLog must publish a sidecar index when a lane closes");
        auto options = logOptions(vlog::VLogReadVisibilityMode::COMMIT_ORDER);
        options.syncMode = vlog::VLogSyncMode::ASYNC;
        options.writeAdmission = vlog::VLogWriteAdmissionMode::PARALLEL;
        options.parallelWriteLanes = 4;
        options.parallelPendingLimitScope = vlog::VLogParallelPendingLimitScope::GLOBAL;
        auto log = vlog::VersionLog::create(path, std::move(options));
        for (uint32_t writer = 0; writer < writerCount; ++writer) {
            const std::string key = "true-parallel-key-" + std::to_string(writer);
            const auto history = log->history(bytes(key));
            require(history.size() == entriesPerWriter, "parallel VLog must recover every lane entry from its durable tail");
        }
        log->close();

        constexpr std::string_view serialKey{"parallel-to-serial"};
        constexpr std::string_view serialValue{"serial-value"};
        auto serialOptions = logOptions(vlog::VLogReadVisibilityMode::COMMIT_ORDER);
        auto serialLog = vlog::VersionLog::create(path, std::move(serialOptions));
        serialLog->append(bytes(serialKey), writerCount * entriesPerWriter + 1u, 0, 0, 0, bytes(serialValue));
        serialLog->close();

        auto verifySerialOptions = logOptions(vlog::VLogReadVisibilityMode::COMMIT_ORDER);
        auto verifySerial = vlog::VersionLog::create(path, std::move(verifySerialOptions));
        const auto serialEntry = verifySerial->getAt(bytes(serialKey), writerCount * entriesPerWriter + 1u);
        require(serialEntry.has_value() && serialEntry->value == std::vector<uint8_t>{serialValue.begin(), serialValue.end()},
                "serial reopening must safely continue from a parallel durable tail");
        verifySerial->close();
    }

    [[nodiscard]] akkaradb::engine::AkkEngineOptions engineOptions(
        const fs::path& dir,
        akkaradb::engine::AkkEngineOptions::WriteAdmissionMode engineAdmission,
        vlog::VLogWriteAdmissionMode logAdmission
    ) {
        using EngineOptions = akkaradb::engine::AkkEngineOptions;
        auto options = EngineOptions{};
        options.paths.dataDir = dir;
        options.components.walEnabled = false;
        options.components.blobEnabled = false;
        options.components.manifestEnabled = false;
        options.components.sstEnabled = false;
        options.components.versionLogEnabled = true;
        options.runtime.writeAdmission = engineAdmission;
        options.runtime.writeVisibility = EngineOptions::WriteVisibilityMode::COMMIT_ORDER;
        options.runtime.visibility.readVisibility = EngineOptions::ReadVisibilityMode::COMMIT_ORDER;
        options.memtable.thresholdBytesPerShard = 0;
        options.vlog.writeAdmission = logAdmission;
        options.vlog.readVisibility = vlog::VLogReadVisibilityMode::COMMIT_ORDER;
        options.vlog.syncMode = vlog::VLogSyncMode::ASYNC;
        return options;
    }

    void verifyEngineBackgroundRecovery(const fs::path& dir) {
        using EngineOptions = akkaradb::engine::AkkEngineOptions;
        auto options = engineOptions(
            dir,
            EngineOptions::WriteAdmissionMode::SERIAL,
            vlog::VLogWriteAdmissionMode::SERIAL
        );
        options.vlog.recoveryMode = vlog::VLogRecoveryMode::BACKGROUND;
        auto engine = akkaradb::engine::AkkEngine::open(options);

        constexpr std::string_view key{"engine-background-key"};
        constexpr std::string_view value{"value"};
        // Both calls cross the VersionLog recovery barrier before touching engine state.
        engine->put(bytes(key), bytes(value));
        const auto observed = engine->get(bytes(key));
        require(observed.has_value() && *observed == std::vector<uint8_t>{value.begin(), value.end()}, "engine get/put must complete after background VersionLog recovery");
        require(engine->history(bytes(key)).size() == 1, "engine background recovery must retain the new VersionLog record");
        engine->close();
    }

    void verifyEngineAdmission(
        const fs::path& dir,
        akkaradb::engine::AkkEngineOptions::WriteAdmissionMode engineAdmission,
        vlog::VLogWriteAdmissionMode logAdmission
    ) {
        auto options = engineOptions(dir, engineAdmission, logAdmission);
        auto engine = akkaradb::engine::AkkEngine::open(options);

        constexpr uint32_t writerCount = 8;
        std::vector<std::string> keys;
        keys.reserve(writerCount);
        for (uint32_t writer = 0; writer < writerCount; ++writer) { keys.push_back("parallel-vlog-" + std::to_string(writer)); }

        std::atomic<uint32_t> ready{0};
        std::atomic<bool> start{false};
        std::exception_ptr failure;
        std::mutex failureMu;
        std::vector<std::thread> writers;
        writers.reserve(writerCount);
        for (uint32_t writer = 0; writer < writerCount; ++writer) {
            writers.emplace_back([&, writer] {
                ready.fetch_add(1, std::memory_order_release);
                while (!start.load(std::memory_order_acquire)) { std::this_thread::yield(); }
                try {
                    const std::string value = "value-" + std::to_string(writer);
                    engine->put(bytes(keys[writer]), bytes(value));
                }
                catch (...) {
                    std::lock_guard lock{failureMu};
                    if (!failure) { failure = std::current_exception(); }
                }
            });
        }
        while (ready.load(std::memory_order_acquire) != writerCount) { std::this_thread::yield(); }
        start.store(true, std::memory_order_release);
        for (auto& writer : writers) { writer.join(); }
        if (failure) { std::rethrow_exception(failure); }

        for (uint32_t writer = 0; writer < writerCount; ++writer) {
            const auto history = engine->history(bytes(keys[writer]));
            require(history.size() == 1, "VersionLog writes must publish one history entry per key");
            const auto observed = engine->getAt(bytes(keys[writer]), history.front().seq);
            require(observed.has_value(), "VersionLog writes must be readable at their committed sequence");
        }
        engine->close();
    }

    void verifyIndependentAdmission(const fs::path& dir) {
        using EngineOptions = akkaradb::engine::AkkEngineOptions;
        verifyEngineAdmission(
            dir / "serial-memtable-serial-vlog",
            EngineOptions::WriteAdmissionMode::SERIAL,
            vlog::VLogWriteAdmissionMode::SERIAL
        );
        verifyEngineAdmission(
            dir / "serial-memtable-prepare-parallel-vlog",
            EngineOptions::WriteAdmissionMode::SERIAL,
            vlog::VLogWriteAdmissionMode::PREPARE_PARALLEL
        );
        verifyEngineAdmission(
            dir / "parallel-memtable-serial-vlog",
            EngineOptions::WriteAdmissionMode::PARALLEL,
            vlog::VLogWriteAdmissionMode::SERIAL
        );
        verifyEngineAdmission(
            dir / "parallel-memtable-parallel-vlog",
            EngineOptions::WriteAdmissionMode::PARALLEL,
            vlog::VLogWriteAdmissionMode::PARALLEL
        );
    }
}

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();

    try {
        TempDir dir;
        verifyReadVisibility(dir.path());
        verifyBackgroundRecovery(dir.path());
        verifyZstdCompression(dir.path());
        verifySegmentation(dir.path());
        verifySingleFileIndexFallback(dir.path());
        verifyRetention(dir.path());
        verifyConcurrentRetentionCompaction(dir.path());
        verifyConcurrentReaders(dir.path());
        verifySerialAppendGate(dir.path());
        verifyParallelPersistence(dir.path());
        verifyEngineBackgroundRecovery(dir.path() / "engine-background");
        verifyIndependentAdmission(dir.path() / "engine");
        return 0;
    }
    catch (const std::exception& ex) {
        std::fprintf(stderr, "version log admission/visibility smoke failed: %s\n", ex.what());
        return 1;
    }
}

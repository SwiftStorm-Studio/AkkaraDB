/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "TestErrorHandlers.hpp"

#include "akk/engine/vlog/VersionLog.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
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

    [[nodiscard]] std::string seqValue(uint64_t seq) { return "value-" + std::to_string(seq); }

    [[nodiscard]] std::optional<uint64_t> parseSeqValue(const std::vector<uint8_t>& value) {
        constexpr std::string_view prefix{"value-"};
        if (value.size() <= prefix.size() || !std::equal(prefix.begin(), prefix.end(), value.begin())) { return std::nullopt; }
        uint64_t out = 0;
        for (size_t i = prefix.size(); i < value.size(); ++i) {
            const auto ch = static_cast<char>(value[i]);
            if (ch < '0' || ch > '9') { return std::nullopt; }
            out = out * 10u + static_cast<uint64_t>(ch - '0');
        }
        return out;
    }

    class TempDir {
        public:
            explicit TempDir(std::string_view name) {
                const auto id = std::chrono::steady_clock::now().time_since_epoch().count();
                path_ = fs::temp_directory_path() / (std::string{name} + "-" + std::to_string(id));
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

    [[nodiscard]] vlog::VersionLogOptions serialOptions() {
        vlog::VersionLogOptions options;
        options.syncMode = vlog::VLogSyncMode::SYNC;
        options.readVisibility = vlog::VLogReadVisibilityMode::COMMIT_ORDER;
        return options;
    }

    [[nodiscard]] vlog::VersionLogOptions parallelOptions() {
        vlog::VersionLogOptions options;
        options.syncMode = vlog::VLogSyncMode::ASYNC;
        options.writeAdmission = vlog::VLogWriteAdmissionMode::PARALLEL;
        options.parallelWriteLanes = 4;
        options.parallelPendingLimitScope = vlog::VLogParallelPendingLimitScope::GLOBAL;
        options.asyncMaxPendingBytes = 8ULL * 1024ULL * 1024ULL;
        options.readVisibility = vlog::VLogReadVisibilityMode::COMMIT_ORDER;
        return options;
    }

    template <typename Operation>
    void requireRuntimeError(Operation&& operation, const char* message) {
        try {
            operation();
        }
        catch (const std::runtime_error&) { return; }
        throw std::runtime_error(message);
    }

    void flipByte(const fs::path& path, std::streamoff offset) {
        std::fstream file(path, std::ios::binary | std::ios::in | std::ios::out);
        require(file.good(), "test setup must open file for byte corruption");
        file.seekg(offset);
        char byte = 0;
        file.get(byte);
        require(file.good(), "test setup must read byte for corruption");
        byte = static_cast<char>(byte ^ 0x01);
        file.seekp(offset);
        file.put(byte);
        require(file.good(), "test setup must write corrupted byte");
    }

    void appendGarbageTail(const fs::path& path) {
        std::ofstream file(path, std::ios::binary | std::ios::app);
        require(file.good(), "test setup must open file for garbage tail append");
        const char garbage[] = {'b', 'a', 'd', '-', 't', 'a', 'i', 'l'};
        file.write(garbage, sizeof(garbage));
        require(file.good(), "test setup must append garbage tail");
    }

    void publishMax(std::atomic<uint64_t>& target, uint64_t value) {
        uint64_t current = target.load(std::memory_order_acquire);
        while (current < value && !target.compare_exchange_weak(current, value, std::memory_order_release, std::memory_order_acquire)) {}
    }

    [[nodiscard]] fs::path firstNonEmptyParallelSegment(const fs::path& dir, std::string_view stem) {
        for (const auto& entry : fs::directory_iterator(dir)) {
            if (!entry.is_regular_file()) { continue; }
            const auto filename = entry.path().filename().string();
            if (!filename.starts_with(std::string{stem} + "-seg-") || !filename.ends_with(".akvlog")) { continue; }
            std::error_code error;
            if (fs::file_size(entry.path(), error) > 32 && !error) { return entry.path(); }
        }
        return {};
    }

    void verifyCorruptEntryFailsRecovery(const fs::path& dir) {
        const auto path = dir / "corrupt-entry.akvlog";
        {
            auto log = vlog::VersionLog::create(path, serialOptions());
            log->append(bytes("corrupt-key"), 1, 0, 0, 0, bytes("corrupt-value"));
            log->close();
        }

        flipByte(path, 48);
        requireRuntimeError([&] { (void)vlog::VersionLog::create(path, serialOptions()); },
                            "VersionLog recovery must reject a CRC-corrupted entry");
    }

    void verifyTruncatedEntryFailsRecovery(const fs::path& dir) {
        const auto path = dir / "truncated-entry.akvlog";
        {
            auto log = vlog::VersionLog::create(path, serialOptions());
            log->append(bytes("truncated-key"), 1, 0, 0, 0, bytes("truncated-value"));
            log->close();
        }

        std::error_code error;
        const auto size = fs::file_size(path, error);
        require(!error && size > 4, "test setup must read VLog size before truncation");
        fs::resize_file(path, size - 3, error);
        require(!error, "test setup must truncate VLog entry");
        requireRuntimeError([&] { (void)vlog::VersionLog::create(path, serialOptions()); },
                            "VersionLog recovery must reject a truncated committed entry");
    }

    void verifyParallelTailBoundsRecovery(const fs::path& dir) {
        const auto path = dir / "parallel-tail.akvlog";
        constexpr std::string_view key{"parallel-tail-key"};
        {
            auto log = vlog::VersionLog::create(path, parallelOptions());
            log->append(bytes(key), 1, 0, 0, 0, bytes("value-1"));
            log->forceSync();
            log->close();
        }

        const auto lanePath = firstNonEmptyParallelSegment(dir, "parallel-tail");
        require(!lanePath.empty(), "parallel VLog must create a non-empty lane segment for tail test");
        appendGarbageTail(lanePath);

        auto options = parallelOptions();
        options.recoveryMode = vlog::VLogRecoveryMode::BACKGROUND;
        auto log = vlog::VersionLog::create(path, std::move(options));
        const auto observed = log->getAt(bytes(key), 1);
        require(observed.has_value() && observed->value == std::vector<uint8_t>{'v', 'a', 'l', 'u', 'e', '-', '1'},
                "parallel VLog recovery must ignore bytes beyond the durable tail");
        require(log->history(bytes(key)).size() == 1, "garbage beyond the durable tail must not create history entries");
        const auto snapshot = log->snapshot();
        require(snapshot.recoveredSegmentCount != 0, "recovery stats must report recovered segments");
        require(snapshot.recoveredEntryCount == 1, "recovery stats must report entries bounded by the durable tail");
        log->close();
    }

    void verifySidecarCorruptionFallsBack(const fs::path& dir) {
        const auto path = dir / "sidecar-fallback.akvlog";
        constexpr std::string_view key{"sidecar-key"};
        {
            auto options = serialOptions();
            options.segmentBytes = 192;
            auto log = vlog::VersionLog::create(path, std::move(options));
            for (uint64_t seq = 1; seq <= 8; ++seq) { log->append(bytes(key), seq, 0, 0, 0, bytes(seqValue(seq))); }
            log->close();
        }

        auto log = vlog::VersionLog::create(path, serialOptions());
        const auto index = dir / "sidecar-fallback.akvidx";
        require(fs::exists(index), "test setup must have a generated sidecar index");
        flipByte(index, static_cast<std::streamoff>(fs::file_size(index) - 1));
        const auto history = log->history(bytes(key));
        require(history.size() == 8, "corrupt sidecar index must fall back to authoritative VLog scan");
        require(history.back().seq == 8, "sidecar fallback must preserve the newest version");
        require(log->snapshot().sidecarFallbackCount != 0, "sidecar fallback stats must report corrupt sidecar use");
        log->close();
    }

    void verifyConcurrentParallelReadWriteStress(const fs::path& dir) {
        const auto path = dir / "parallel-stress.akvlog";
        constexpr std::string_view sharedKey{"parallel-stress-shared-key"};
        constexpr uint32_t writerCount = 8;
        constexpr uint32_t entriesPerWriter = 128;
        constexpr uint32_t readerCount = 6;
        constexpr uint32_t readerIterations = 256;
        constexpr uint64_t totalEntries = static_cast<uint64_t>(writerCount) * entriesPerWriter;

        auto log = vlog::VersionLog::create(path, parallelOptions());
        std::atomic<uint64_t> nextSeq{1};
        std::atomic<uint64_t> highestSubmitted{0};
        std::atomic<uint32_t> ready{0};
        std::atomic<bool> start{false};
        std::exception_ptr failure;
        std::mutex failureMu;
        const auto recordFailure = [&](std::exception_ptr error) {
            std::lock_guard lock{failureMu};
            if (!failure) { failure = std::move(error); }
        };

        std::vector<std::thread> writers;
        writers.reserve(writerCount);
        for (uint32_t writer = 0; writer < writerCount; ++writer) {
            writers.emplace_back([&] {
                ready.fetch_add(1, std::memory_order_release);
                while (!start.load(std::memory_order_acquire)) { std::this_thread::yield(); }
                try {
                    for (uint32_t i = 0; i < entriesPerWriter; ++i) {
                        const uint64_t seq = nextSeq.fetch_add(1, std::memory_order_acq_rel);
                        const auto value = seqValue(seq);
                        log->append(bytes(sharedKey), seq, 0, 0, 0, bytes(value));
                        publishMax(highestSubmitted, seq);
                    }
                }
                catch (...) { recordFailure(std::current_exception()); }
            });
        }

        std::vector<std::thread> readers;
        readers.reserve(readerCount);
        for (uint32_t reader = 0; reader < readerCount; ++reader) {
            readers.emplace_back([&] {
                ready.fetch_add(1, std::memory_order_release);
                while (!start.load(std::memory_order_acquire)) { std::this_thread::yield(); }
                try {
                    for (uint32_t iteration = 0; iteration < readerIterations; ++iteration) {
                        const uint64_t target = highestSubmitted.load(std::memory_order_acquire);
                        const auto history = log->history(bytes(sharedKey));
                        uint64_t previous = 0;
                        for (const auto& entry : history) {
                            require(entry.seq > previous, "parallel history must remain strictly sorted during concurrent access");
                            const auto parsed = parseSeqValue(entry.value);
                            require(parsed.has_value() && *parsed == entry.seq, "parallel history must retain each entry value");
                            previous = entry.seq;
                        }
                        if (target != 0) {
                            const auto observed = log->getAt(bytes(sharedKey), target);
                            if (observed.has_value()) {
                                require(observed->seq <= target, "parallel getAt must not return a version after the requested sequence");
                                const auto parsed = parseSeqValue(observed->value);
                                require(parsed.has_value() && *parsed == observed->seq, "parallel getAt must return the matching value");
                            }
                        }
                    }
                }
                catch (...) { recordFailure(std::current_exception()); }
            });
        }

        while (ready.load(std::memory_order_acquire) != writerCount + readerCount) { std::this_thread::yield(); }
        start.store(true, std::memory_order_release);
        for (auto& writer : writers) { writer.join(); }
        for (auto& reader : readers) { reader.join(); }
        if (failure) { std::rethrow_exception(failure); }

        log->forceSync();
        const auto parallelSnapshot = log->snapshot();
        require(parallelSnapshot.parallelLaneCount == 4, "parallel stats must report configured lane count");
        require(parallelSnapshot.parallelPendingWrites == 0, "forceSync must drain parallel pending writes");
        require(parallelSnapshot.parallelPendingBytes == 0, "forceSync must drain parallel pending bytes");
        const auto beforeClose = log->history(bytes(sharedKey));
        require(beforeClose.size() == totalEntries, "parallel stress must publish every written history entry before close");
        log->close();

        auto recovered = vlog::VersionLog::create(path, parallelOptions());
        const auto recoverySnapshot = recovered->snapshot();
        require(recoverySnapshot.recoveredEntryCount == totalEntries, "parallel recovery stats must report recovered entry count");
        require(recoverySnapshot.recoveredSegmentCount != 0, "parallel recovery stats must report recovered segments");
        const auto history = recovered->history(bytes(sharedKey));
        require(history.size() == totalEntries, "parallel stress recovery must retain every written history entry");
        for (uint64_t i = 0; i < history.size(); ++i) {
            const uint64_t expectedSeq = i + 1u;
            require(history[i].seq == expectedSeq, "parallel stress recovery must preserve sequence ordering");
            const auto parsed = parseSeqValue(history[i].value);
            require(parsed.has_value() && *parsed == expectedSeq, "parallel stress recovery must preserve values");
        }
        recovered->close();
    }
}

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();

    try {
        TempDir dir{"akkaradb-vlog-recovery-concurrency"};
        verifyCorruptEntryFailsRecovery(dir.path());
        verifyTruncatedEntryFailsRecovery(dir.path());
        verifyParallelTailBoundsRecovery(dir.path());
        verifySidecarCorruptionFallsBack(dir.path());
        verifyConcurrentParallelReadWriteStress(dir.path());
        return 0;
    }
    catch (const std::exception& ex) {
        std::fprintf(stderr, "version log recovery/concurrency smoke failed: %s\n", ex.what());
        return 1;
    }
}

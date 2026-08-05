/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// benchmarks/smoke/version_log_fault_injection_smoke_test.cpp
#include "TestErrorHandlers.hpp"

#include "akk/cpu/CRC32C.hpp"
#include "akk/engine/vlog/VersionLog.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <filesystem>
#include <fstream>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {
    namespace fs = std::filesystem;
    namespace vlog = akkaradb::engine::vlog;

    constexpr uint32_t AKVLOG_TAIL_MAGIC = 0x54564B41u;
    constexpr uint16_t AKVLOG_TAIL_VERSION = 0x0001u;

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
                path_ = fs::temp_directory_path() / ("akkaradb-vlog-faults-" + std::to_string(id));
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

    [[nodiscard]] vlog::VersionLogOptions serialSyncOptions() {
        vlog::VersionLogOptions options;
        options.syncMode = vlog::VLogSyncMode::SYNC;
        options.readVisibility = vlog::VLogReadVisibilityMode::COMMIT_ORDER;
        return options;
    }

    [[nodiscard]] vlog::VersionLogOptions serialAsyncOptions() {
        vlog::VersionLogOptions options;
        options.syncMode = vlog::VLogSyncMode::ASYNC;
        options.writeAdmission = vlog::VLogWriteAdmissionMode::SERIAL;
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

    void appendU16Le(std::vector<uint8_t>& out, uint16_t value) {
        out.push_back(static_cast<uint8_t>(value & 0xffu));
        out.push_back(static_cast<uint8_t>((value >> 8u) & 0xffu));
    }

    void appendU32Le(std::vector<uint8_t>& out, uint32_t value) {
        for (uint32_t shift = 0; shift < 32u; shift += 8u) { out.push_back(static_cast<uint8_t>((value >> shift) & 0xffu)); }
    }

    void appendU64Le(std::vector<uint8_t>& out, uint64_t value) {
        for (uint32_t shift = 0; shift < 64u; shift += 8u) { out.push_back(static_cast<uint8_t>((value >> shift) & 0xffu)); }
    }

    void writeU32LeAt(std::vector<uint8_t>& out, size_t offset, uint32_t value) {
        out[offset] = static_cast<uint8_t>(value & 0xffu);
        out[offset + 1u] = static_cast<uint8_t>((value >> 8u) & 0xffu);
        out[offset + 2u] = static_cast<uint8_t>((value >> 16u) & 0xffu);
        out[offset + 3u] = static_cast<uint8_t>((value >> 24u) & 0xffu);
    }

    [[nodiscard]] fs::path tailPathFor(const fs::path& segmentPath) {
        auto tail = segmentPath;
        tail.replace_extension(".akvtail");
        return tail;
    }

    [[nodiscard]] fs::path segmentPathFor(const fs::path& logPath, uint64_t id) {
        if (id == 0) { return logPath; }
        return logPath.parent_path() / (logPath.stem().string() + "-seg-" + std::to_string(id) + logPath.extension().string());
    }

    [[nodiscard]] uint64_t highestSegmentId(const fs::path& logPath) {
        uint64_t highest = fs::exists(logPath) ? 0 : 0;
        const auto parent = logPath.parent_path().empty() ? fs::path{"."} : logPath.parent_path();
        const std::string prefix = logPath.stem().string() + "-seg-";
        const std::string extension = logPath.extension().string();
        for (const auto& entry : fs::directory_iterator(parent)) {
            const auto name = entry.path().filename().string();
            if (!name.starts_with(prefix) || !name.ends_with(extension)) { continue; }
            const std::string suffix = name.substr(prefix.size(), name.size() - prefix.size() - extension.size());
            if (!std::ranges::all_of(suffix, [](unsigned char ch) { return ch >= '0' && ch <= '9'; })) { continue; }
            highest = std::max(highest, static_cast<uint64_t>(std::stoull(suffix)));
        }
        return highest;
    }

    void writeBytes(const fs::path& path, std::span<const uint8_t> data, bool append = false) {
        std::ofstream file(path, std::ios::binary | (append ? std::ios::app : std::ios::trunc));
        require(file.good(), "test setup must open file for byte write");
        file.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
        require(file.good(), "test setup must write bytes");
    }

    void writeText(const fs::path& path, std::string_view text, bool append = false) {
        writeBytes(path, bytes(text), append);
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

    void writeValidTail(const fs::path& segmentPath, uint64_t committedBytes) {
        std::vector<uint8_t> encoded;
        encoded.reserve(24);
        appendU32Le(encoded, AKVLOG_TAIL_MAGIC);
        appendU16Le(encoded, AKVLOG_TAIL_VERSION);
        appendU16Le(encoded, 0);
        appendU64Le(encoded, committedBytes);
        appendU32Le(encoded, 0);
        appendU32Le(encoded, 0);
        const uint32_t crc = akkaradb::cpu::CRC32C(reinterpret_cast<const std::byte*>(encoded.data()), encoded.size());
        writeU32LeAt(encoded, 16, crc);
        writeBytes(tailPathFor(segmentPath), encoded);
    }

    void createLog(const fs::path& path, vlog::VersionLogOptions options, uint64_t entries = 4) {
        auto log = vlog::VersionLog::create(path, std::move(options));
        for (uint64_t seq = 1; seq <= entries; ++seq) {
            const auto value = "value-" + std::to_string(seq);
            log->append(bytes("fault-key"), seq, 0, 0, 0, bytes(value));
        }
        log->close();
    }

    void verifyCorruptTailFailsRecovery(const fs::path& dir) {
        const auto path = dir / "corrupt-tail.akvlog";
        createLog(path, serialSyncOptions());
        const auto tail = tailPathFor(path);
        require(fs::exists(tail), "test setup must create a durable tail");
        flipByte(tail, 8);
        requireRuntimeError([&] { (void)vlog::VersionLog::create(path, serialSyncOptions()); },
                            "VersionLog recovery must reject a corrupt durable tail");
    }

    void verifyInvalidTailBeyondSegmentFailsRecovery(const fs::path& dir) {
        const auto path = dir / "tail-too-long.akvlog";
        createLog(path, serialSyncOptions());
        const uint64_t size = fs::file_size(path);
        writeValidTail(path, size + 4096);
        requireRuntimeError([&] { (void)vlog::VersionLog::create(path, serialSyncOptions()); },
                            "VersionLog recovery must reject a durable tail beyond the segment size");
    }

    void verifyStaleTailTempIsIgnored(const fs::path& dir) {
        const auto path = dir / "tail-temp.akvlog";
        createLog(path, serialSyncOptions());
        writeText(tailPathFor(path).string() + ".tmp", "not-a-valid-tail");
        auto log = vlog::VersionLog::create(path, serialSyncOptions());
        require(log->history(bytes("fault-key")).size() == 4, "stale durable-tail temporary files must not affect recovery");
        log->close();
    }

    void verifyPartialActiveHeaderIsRepaired(const fs::path& dir, std::string_view name, vlog::VersionLogOptions options) {
        const auto path = dir / (std::string{name} + ".akvlog");
        options.segmentBytes = 180;
        createLog(path, options, 8);

        const uint64_t partialId = highestSegmentId(path) + 1u;
        const auto partialPath = segmentPathFor(path, partialId);
        const std::array<uint8_t, 7> partialHeader{{'A', 'K', 'V', '5', 1, 0, 0}};
        writeBytes(partialPath, partialHeader);

        auto log = vlog::VersionLog::create(path, options);
        require(log->history(bytes("fault-key")).size() == 8, "partial active headers must not hide previously durable history");
        log->append(bytes("after-partial-header"), 9, 0, 0, 0, bytes("value-9"));
        const auto observed = log->getAt(bytes("after-partial-header"), 9);
        require(observed.has_value() && observed->value == std::vector<uint8_t>{'v', 'a', 'l', 'u', 'e', '-', '9'},
                "repaired partial active headers must accept subsequent appends");
        log->close();
        require(fs::file_size(partialPath) > 32, "repaired partial active segment must contain a complete header and appended entry");
    }

    void verifySerialAsyncMissingTailUsesValidatedPrefix(const fs::path& dir) {
        const auto path = dir / "missing-tail-prefix.akvlog";
        auto options = serialAsyncOptions();
        createLog(path, options, 1);
        const uint64_t validBytes = fs::file_size(path);
        std::error_code error;
        fs::remove(tailPathFor(path), error);
        require(!error, "test setup must remove durable tail");
        writeText(path, "interrupted-entry-bytes", true);

        auto log = vlog::VersionLog::create(path, options);
        require(log->history(bytes("fault-key")).size() == 1, "missing tail recovery must keep only the validated prefix");
        log->close();
        require(fs::file_size(path) == validBytes, "missing tail recovery must truncate interrupted bytes");
    }
}

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();

    try {
        TempDir dir;
        verifyCorruptTailFailsRecovery(dir.path());
        verifyInvalidTailBeyondSegmentFailsRecovery(dir.path());
        verifyStaleTailTempIsIgnored(dir.path());
        verifyPartialActiveHeaderIsRepaired(dir.path(), "partial-active-header-sync", serialSyncOptions());
        verifyPartialActiveHeaderIsRepaired(dir.path(), "partial-active-header-async", serialAsyncOptions());
        verifySerialAsyncMissingTailUsesValidatedPrefix(dir.path());
        return 0;
    }
    catch (const std::exception& ex) {
        std::fprintf(stderr, "version log fault-injection smoke failed: %s\n", ex.what());
        return 1;
    }
}

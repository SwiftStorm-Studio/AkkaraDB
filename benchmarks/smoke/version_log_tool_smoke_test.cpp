/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "TestErrorHandlers.hpp"
#include "tools/VLogToolCore.hpp"

#include "akk/engine/vlog/VersionLog.hpp"

#include <chrono>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {
    namespace fs = std::filesystem;
    namespace tool = akkaradb::tools::vlogtool;
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
                path_ = fs::temp_directory_path() / ("akkaradb-vlog-tool-" + std::to_string(id));
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

    [[nodiscard]] int runTool(std::vector<std::string> args, std::ostringstream& out, std::ostringstream& err) {
        std::vector<char*> argv;
        argv.reserve(args.size());
        for (auto& arg : args) { argv.push_back(arg.data()); }
        return tool::run(static_cast<int>(argv.size()), argv.data(), out, err);
    }

    void createLog(const fs::path& path) {
        vlog::VersionLogOptions options;
        options.logPath = path;
        options.syncMode = vlog::VLogSyncMode::SYNC;
        auto log = vlog::VersionLog::create(std::move(options));
        for (uint64_t seq = 1; seq <= 4; ++seq) {
            const auto value = "value-" + std::to_string(seq);
            log->append(bytes("tool-key"), seq, 0, 0, 0, bytes(value));
        }
        log->close();
    }

    void verifyValidateAndRebuild(const fs::path& dir) {
        const auto logPath = dir / "tool.akvlog";
        const auto indexPath = dir / "tool.akvidx";
        createLog(logPath);
        require(fs::exists(indexPath), "test setup must create a derived VLog sidecar");

        std::error_code error;
        fs::remove(indexPath, error);
        require(!error && !fs::exists(indexPath), "test setup must remove the derived VLog sidecar");

        std::ostringstream validateOut;
        std::ostringstream validateErr;
        const int validateCode = runTool(
            {"akkaradb_vlog_tool", "validate", "--log", logPath.string(), "--json"},
            validateOut,
            validateErr
        );
        require(validateCode == 0, "validate must succeed for a healthy VersionLog");
        require(validateErr.str().empty(), "validate must not write stderr for a healthy VersionLog");
        require(validateOut.str().find("\"ok\":true") != std::string::npos, "validate JSON must report success");
        require(validateOut.str().find("\"recoveredEntryCount\":4") != std::string::npos, "validate JSON must report recovered entries");
        require(fs::exists(indexPath), "validate must rebuild a missing derived VLog sidecar");

        fs::remove(indexPath, error);
        require(!error && !fs::exists(indexPath), "test setup must remove the sidecar before rebuild-indexes");

        std::ostringstream rebuildOut;
        std::ostringstream rebuildErr;
        const int rebuildCode = runTool(
            {"akkaradb_vlog_tool", "rebuild-indexes", "--log", logPath.string()},
            rebuildOut,
            rebuildErr
        );
        require(rebuildCode == 0, "rebuild-indexes must succeed for a healthy VersionLog");
        require(rebuildErr.str().empty(), "rebuild-indexes must not write stderr for a healthy VersionLog");
        require(rebuildOut.str().find("VersionLog OK") != std::string::npos, "rebuild-indexes text output must report success");
        require(fs::exists(indexPath), "rebuild-indexes must recreate the derived VLog sidecar");
    }

    void verifyMissingLogFailsWithoutCreating(const fs::path& dir) {
        const auto missingPath = dir / "missing.akvlog";
        std::ostringstream out;
        std::ostringstream err;
        const int code = runTool({"akkaradb_vlog_tool", "validate", "--log", missingPath.string()}, out, err);
        require(code == 1, "validate must fail when no base log or segment exists");
        require(out.str().empty(), "missing log validation must not write success output");
        require(err.str().find("does not exist") != std::string::npos, "missing log validation must explain the failure");
        require(!fs::exists(missingPath), "missing log validation must not create a new empty log");
    }
}

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();

    try {
        TempDir dir;
        verifyValidateAndRebuild(dir.path());
        verifyMissingLogFailsWithoutCreating(dir.path());
        return 0;
    }
    catch (const std::exception& ex) {
        std::cerr << "version log tool smoke failed: " << ex.what() << '\n';
        return 1;
    }
}

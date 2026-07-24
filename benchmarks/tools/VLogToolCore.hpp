/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#pragma once

#include "akk/engine/vlog/VersionLog.hpp"

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <ostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace akkaradb::tools::vlogtool {
    namespace fs = std::filesystem;
    namespace vlog = akkaradb::engine::vlog;

    struct Args {
        std::string command;
        fs::path logPath;
        bool json = false;
        bool help = false;
    };

    inline void printUsage(std::ostream& out) {
        out << "Usage:\n"
            << "  akkaradb_vlog_tool validate --log <path> [--json]\n"
            << "  akkaradb_vlog_tool rebuild-indexes --log <path> [--json]\n\n"
            << "Commands:\n"
            << "  validate         Open and validate the authoritative VersionLog segments.\n"
            << "  rebuild-indexes  Validate segments and regenerate derived .akvidx sidecars.\n";
    }

    [[nodiscard]] inline Args parseArgs(int argc, char** argv) {
        Args args;
        if (argc < 2) { throw std::invalid_argument("missing command"); }
        args.command = argv[1];
        for (int i = 2; i < argc; ++i) {
            const std::string_view token{argv[i]};
            if (token == "--log") {
                if (i + 1 >= argc) { throw std::invalid_argument("--log requires a path"); }
                args.logPath = argv[++i];
            }
            else if (token == "--json") { args.json = true; }
            else if (token == "--help" || token == "-h") { args.help = true; }
            else { throw std::invalid_argument("unknown argument: " + std::string{token}); }
        }
        if (args.help) { return args; }
        if (args.command != "validate" && args.command != "rebuild-indexes") {
            throw std::invalid_argument("unknown command: " + args.command);
        }
        if (args.logPath.empty()) { throw std::invalid_argument("--log is required"); }
        return args;
    }

    [[nodiscard]] inline bool hasAnySegment(const fs::path& logPath) {
        std::error_code error;
        if (fs::exists(logPath, error)) { return true; }
        if (error) { throw std::runtime_error("cannot inspect log path: " + error.message()); }
        const auto parent = logPath.parent_path().empty() ? fs::current_path() : logPath.parent_path();
        const auto stem = logPath.stem().string();
        const auto extension = logPath.extension().string();
        if (!fs::exists(parent, error)) { return false; }
        if (error) { throw std::runtime_error("cannot inspect log directory: " + error.message()); }
        for (const auto& entry : fs::directory_iterator(parent, error)) {
            if (error) { throw std::runtime_error("cannot scan log directory: " + error.message()); }
            if (!entry.is_regular_file(error)) {
                error.clear();
                continue;
            }
            const auto name = entry.path().filename().string();
            if (name.rfind(stem + "-seg-", 0) == 0 && entry.path().extension().string() == extension) { return true; }
        }
        return false;
    }

    inline void printSnapshotText(std::ostream& out, const vlog::VersionLogSnapshot& snapshot) {
        out << "VersionLog OK\n"
            << "segments=" << snapshot.segmentCount << '\n'
            << "activeSegmentBytes=" << snapshot.activeSegmentBytes << '\n'
            << "durableBytes=" << snapshot.durableBytes << '\n'
            << "indexedEntries=" << snapshot.indexedEntries << '\n'
            << "rollbackEntries=" << snapshot.rollbackEntries << '\n'
            << "recoveryDurationMicros=" << snapshot.recoveryDurationMicros << '\n'
            << "recoveredSegmentCount=" << snapshot.recoveredSegmentCount << '\n'
            << "recoveredEntryCount=" << snapshot.recoveredEntryCount << '\n'
            << "sidecarRebuildFailures=" << snapshot.sidecarRebuildFailures << '\n';
    }

    inline void printSnapshotJson(std::ostream& out, const vlog::VersionLogSnapshot& snapshot) {
        out << "{"
            << "\"ok\":true,"
            << "\"segments\":" << snapshot.segmentCount << ','
            << "\"activeSegmentBytes\":" << snapshot.activeSegmentBytes << ','
            << "\"durableBytes\":" << snapshot.durableBytes << ','
            << "\"indexedEntries\":" << snapshot.indexedEntries << ','
            << "\"rollbackEntries\":" << snapshot.rollbackEntries << ','
            << "\"recoveryDurationMicros\":" << snapshot.recoveryDurationMicros << ','
            << "\"recoveredSegmentCount\":" << snapshot.recoveredSegmentCount << ','
            << "\"recoveredEntryCount\":" << snapshot.recoveredEntryCount << ','
            << "\"sidecarRebuildFailures\":" << snapshot.sidecarRebuildFailures
            << "}\n";
    }

    [[nodiscard]] inline int run(int argc, char** argv, std::ostream& out, std::ostream& err) {
        try {
            const Args args = parseArgs(argc, argv);
            if (args.help) {
                printUsage(out);
                return 0;
            }
            if (!hasAnySegment(args.logPath)) {
                throw std::runtime_error("VersionLog file or segment set does not exist: " + args.logPath.string());
            }

            vlog::VersionLogOptions options;
            options.logPath = args.logPath;
            options.recoveryMode = vlog::VLogRecoveryMode::EAGER;
            auto log = vlog::VersionLog::create(std::move(options));
            log->forceSync();
            const auto snapshot = log->snapshot();
            log->close();

            if (args.json) { printSnapshotJson(out, snapshot); }
            else { printSnapshotText(out, snapshot); }
            if (args.command == "rebuild-indexes" && snapshot.sidecarRebuildFailures != 0) { return 2; }
            return 0;
        }
        catch (const std::exception& ex) {
            err << "akkaradb_vlog_tool: " << ex.what() << '\n';
            return 1;
        }
    }
}

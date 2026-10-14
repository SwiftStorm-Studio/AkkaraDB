/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
#pragma once

#include "akk/engine/wal/WalFraming.hpp"
#include <array>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <optional>
#include <stdexcept>
#ifdef _WIN32
#include <io.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace akkaradb::engine::wal::detail {
    inline std::filesystem::path checkpointPath(std::filesystem::path segment) {
        segment += ".checkpoint";
        return segment;
    }

    inline std::optional<WalSegmentHeader> readCheckpoint(const std::filesystem::path& segment) {
        const auto path = checkpointPath(segment);
        if (!std::filesystem::exists(path)) { return {}; }
        std::array<uint8_t, WalSegmentHeader::SIZE> bytes{};
        std::ifstream input{path, std::ios::binary};
        if (std::filesystem::file_size(path) != bytes.size() ||
            !input.read(reinterpret_cast<char*>(bytes.data()), bytes.size())) {
            throw std::runtime_error("WAL: corrupt segment checkpoint at " + path.string());
        }
        const auto header = WalSegmentHeader::deserialize(bytes.data());
        if (!header.verifyMagic() || !header.verifyVersion() || !header.verifyChecksum() || header.publishedBytes < WalSegmentHeader::SIZE) {
            throw std::runtime_error("WAL: corrupt segment checkpoint at " + path.string());
        }
        return header;
    }

    inline void syncCheckpointDirectory(const std::filesystem::path& directory) {
#ifndef _WIN32
        const int descriptor = ::open(directory.c_str(), O_RDONLY | O_DIRECTORY);
        if (descriptor < 0) { throw std::runtime_error("WAL: failed to open checkpoint directory"); }
        const int result = ::fsync(descriptor); ::close(descriptor);
        if (result != 0) { throw std::runtime_error("WAL: failed to sync checkpoint directory"); }
#else
        (void)directory; // Atomic replacements below use MOVEFILE_WRITE_THROUGH.
#endif
    }

    inline void replaceCheckpoint(const std::filesystem::path& from, const std::filesystem::path& to) {
#ifdef _WIN32
        if (!::MoveFileExW(from.c_str(), to.c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
            throw std::runtime_error("WAL: failed to publish segment checkpoint");
        }
#else
        std::filesystem::rename(from, to);
        syncCheckpointDirectory(to.parent_path());
#endif
    }

    inline void publishCheckpoint(const std::filesystem::path& segment, const WalSegmentHeader& header) {
        const auto path = checkpointPath(segment);
        auto temporary = path; temporary += ".tmp";
#ifdef _WIN32
        FILE* output = ::_wfopen(temporary.c_str(), L"wb");
#else
        FILE* output = std::fopen(temporary.c_str(), "wb");
#endif
        if (!output) { throw std::runtime_error("WAL: failed to create segment checkpoint"); }
        try {
            std::array<uint8_t, WalSegmentHeader::SIZE> bytes{}; header.serialize(bytes.data());
            if (std::fwrite(bytes.data(), 1, bytes.size(), output) != bytes.size() || std::fflush(output) != 0) {
                throw std::runtime_error("WAL: failed to write segment checkpoint");
            }
#ifdef _WIN32
            if (::_commit(::_fileno(output)) != 0) { throw std::runtime_error("WAL: failed to sync segment checkpoint"); }
#else
            if (::fsync(::fileno(output)) != 0) { throw std::runtime_error("WAL: failed to sync segment checkpoint"); }
#endif
            const int result = std::fclose(output); output = nullptr;
            if (result != 0) { throw std::runtime_error("WAL: failed to close segment checkpoint"); }
            replaceCheckpoint(temporary, path);
        } catch (...) {
            if (output) { std::fclose(output); }
            std::error_code ignored; std::filesystem::remove(temporary, ignored);
            throw;
        }
    }

    inline void syncRepairedSegment(const std::filesystem::path& segment) {
#ifdef _WIN32
        FILE* file = ::_wfopen(segment.c_str(), L"r+b");
#else
        FILE* file = std::fopen(segment.c_str(), "r+b");
#endif
        if (!file) { throw std::runtime_error("WAL: failed to open repaired segment"); }
#ifdef _WIN32
        const int result = ::_commit(::_fileno(file));
#else
        const int result = ::fsync(::fileno(file));
#endif
        std::fclose(file);
        if (result != 0) { throw std::runtime_error("WAL: failed to sync repaired segment"); }
    }

    inline void retireCheckpoint(const std::filesystem::path& segment) {
        const auto path = checkpointPath(segment);
        if (!std::filesystem::exists(path)) { return; }
        // Make proof retirement durable before pruning its covered WAL file.
        // A crash can leave an extra obsolete WAL, never a proof of a missing file.
        auto retired = path; retired += ".retired";
        replaceCheckpoint(path, retired);
        std::filesystem::remove(retired);
    }
}

/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
#pragma once

#include "akk/engine/wal/WalFraming.hpp"
#include <array>
#include <cstdio>
#include <filesystem>
#include <optional>
#include <stdexcept>
#ifdef _WIN32
#include <io.h>
#include <windows.h>
#else
#include <cerrno>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace akkaradb::engine::wal::detail {
    inline std::filesystem::path checkpointPath(std::filesystem::path segment) {
        segment += ".checkpoint";
        return segment;
    }

    inline std::optional<WalSegmentHeader> readCheckpoint(const std::filesystem::path& segment) {
        const auto path = checkpointPath(segment);
        std::array<uint8_t, WalSegmentHeader::SIZE> bytes{};
#ifdef _WIN32
        // Readers keep the old immutable image alive without preventing an
        // atomic publication or proof retirement on another thread.
        const auto input = ::CreateFileW(path.c_str(), GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
        if (input == INVALID_HANDLE_VALUE) {
            const auto error = ::GetLastError();
            if (error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND) { return {}; }
            throw std::runtime_error("WAL: failed to open segment checkpoint at " + path.string());
        }
        LARGE_INTEGER size{}; DWORD read = 0;
        const bool valid = ::GetFileSizeEx(input, &size) && size.QuadPart == bytes.size() &&
            ::ReadFile(input, bytes.data(), static_cast<DWORD>(bytes.size()), &read, nullptr) && read == bytes.size();
        ::CloseHandle(input);
#else
        FILE* input = std::fopen(path.c_str(), "rb");
        if (!input) {
            if (errno == ENOENT) { return {}; }
            throw std::runtime_error("WAL: failed to open segment checkpoint at " + path.string());
        }
        struct stat size{};
        const bool valid = ::fstat(::fileno(input), &size) == 0 && size.st_size == bytes.size() &&
            std::fread(bytes.data(), 1, bytes.size(), input) == bytes.size();
        std::fclose(input);
#endif
        if (!valid) {
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

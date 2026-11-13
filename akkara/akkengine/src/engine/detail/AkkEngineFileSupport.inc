/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/AkkEngineFileSupport.inc
// Included inside detail::runtime by AkkEngineSupport.hpp.
    [[nodiscard]] inline uint64_t nowNs() noexcept {
        timespec ts{};
        timespec_get(&ts, TIME_UTC);
        return ts.tv_sec * 1'000'000'000ULL + ts.tv_nsec;
    }

    inline void validateRollbackOptions(RollbackOptions options) {
        if (options.execution > RollbackExecutionMode::NEXT_STARTUP) { throw std::invalid_argument("AkkEngine: invalid rollback execution mode"); }
        if (options.conflict > RollbackConflictPolicy::OVERWRITE_LATEST) { throw std::invalid_argument("AkkEngine: invalid rollback conflict policy"); }
    }

    inline void writeFileAtomicallyDurable(const fs::path& path, std::span<const uint8_t> bytes) {
        if (!path.parent_path().empty()) { fs::create_directories(path.parent_path()); }
        auto temporary = path;
        temporary += ".tmp";
        {
            std::ofstream out{temporary, std::ios::binary | std::ios::trunc};
            if (!out || !out.write(reinterpret_cast<const char*>(bytes.data()), std::ssize(bytes))) {
                throw std::runtime_error("AkkEngine: failed to write durable journal");
            }
            out.flush();
            if (!out) { throw std::runtime_error("AkkEngine: failed to flush durable journal"); }
        }
        #ifdef _WIN32
        auto* const file = CreateFileW(
            temporary.c_str(),
            GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ,
            nullptr,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_WRITE_THROUGH,
            nullptr
        );
        if (file == INVALID_HANDLE_VALUE || !FlushFileBuffers(file)) {
            if (file != INVALID_HANDLE_VALUE) { CloseHandle(file); }
            throw std::runtime_error("AkkEngine: failed to sync durable journal");
        }
        CloseHandle(file);
        if (!MoveFileExW(temporary.c_str(), path.c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
            throw std::runtime_error("AkkEngine: failed to publish durable journal");
        }
        #else
        const int fd = open(temporary.c_str(), O_RDONLY); if (fd < 0 || fsync(fd) != 0) {
            if (fd >= 0) { close(fd); }
            throw std::runtime_error("AkkEngine: failed to sync durable journal");
        } close(fd); fs::rename(temporary, path); const fs::path parentPath = path.parent_path().empty() ? fs::path{"."} : path.parent_path(); const int
            parent = open(parentPath.c_str(), O_RDONLY | O_DIRECTORY); if (parent < 0 || fsync(parent) != 0) {
            if (parent >= 0) { close(parent); }
            throw std::runtime_error("AkkEngine: failed to sync durable journal directory");
        } close(parent);
        #endif
    }

    using cluster::detail::rendezvousScore;

    // Test-only fault injection used by the recovery smoke test. The process
    // terminates without unwinding, matching the storage guarantees required
    // at each persistence boundary.
    inline void crashAtTestPoint(const char* point) noexcept {
        if (const auto* requested = std::getenv("AKKARADB_TEST_CRASH_POINT"); requested != nullptr && std::strcmp(requested, point) == 0) {
            std::quick_exit(86);
        }
    }

    inline void corruptSnapshotStagingAtTestPoint(const fs::path& path, const char* point) {
        if (const auto* requested = std::getenv("AKKARADB_TEST_CORRUPT_SNAPSHOT_STAGING_POINT"); requested == nullptr || std::strcmp(requested, point) != 0) {
            return;
        }
        #ifdef _WIN32
        (void)_putenv_s("AKKARADB_TEST_CORRUPT_SNAPSHOT_STAGING_POINT", "");
        #else
        (void)unsetenv("AKKARADB_TEST_CORRUPT_SNAPSHOT_STAGING_POINT");
        #endif

        std::fstream file{path, std::ios::binary | std::ios::in | std::ios::out};
        if (!file) { throw std::runtime_error("AkkEngine: failed to open replication snapshot staging file for test corruption"); }

        constexpr std::streamoff firstRecordHeaderOffset = 5 + sizeof(uint64_t) + sizeof(uint64_t);
        file.seekg(firstRecordHeaderOffset);
        std::array<uint8_t, sizeof(uint32_t)> keyLenBytes{};
        std::array<uint8_t, sizeof(uint64_t)> valueLenBytes{};
        file.read(reinterpret_cast<char*>(keyLenBytes.data()), keyLenBytes.size());
        file.read(reinterpret_cast<char*>(valueLenBytes.data()), valueLenBytes.size());
        if (!file) { throw std::runtime_error("AkkEngine: failed to read replication snapshot staging header for test corruption"); }
        uint32_t keyLen = 0;
        for (size_t i = 0; i < keyLenBytes.size(); ++i) { keyLen |= static_cast<uint32_t>(keyLenBytes[i]) << (i * 8); }
        uint64_t valueLen = 0;
        for (size_t i = 0; i < valueLenBytes.size(); ++i) { valueLen |= static_cast<uint64_t>(valueLenBytes[i]) << (i * 8); }
        if (valueLen == 0) { throw std::runtime_error("AkkEngine: replication snapshot staging value is empty at test corruption point"); }

        constexpr std::streamoff firstValueHeaderSize = sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t);
        const std::streamoff firstValueByteOffset = firstRecordHeaderOffset + firstValueHeaderSize + keyLen;
        file.seekg(firstValueByteOffset);
        char byte = 0;
        file.get(byte);
        if (!file) { throw std::runtime_error("AkkEngine: failed to read replication snapshot staging byte for test corruption"); }
        byte ^= 0x5A;
        file.clear();
        file.seekp(firstValueByteOffset);
        file.put(byte);
        file.flush();
        if (!file) { throw std::runtime_error("AkkEngine: failed to corrupt replication snapshot staging byte"); }

        std::ofstream marker{path.string() + ".corrupted", std::ios::binary | std::ios::trunc};
        marker << "1";
        if (!marker) { throw std::runtime_error("AkkEngine: failed to write replication snapshot staging corruption marker"); }
    }
    inline void ensureDir(const fs::path& path) { if (!path.empty()) { fs::create_directories(path); } }

    inline void removeFileIfExists(const fs::path& path) noexcept {
        if (path.empty()) { return; }
        std::error_code ec;
        fs::remove(path, ec);
    }
    [[nodiscard]] inline uint64_t loadOrCreateNodeId(const fs::path& path) {
        if (path.empty()) { return 0; }
        ensureDir(path.parent_path());

        {
            std::ifstream in(path, std::ios::binary);
            uint64_t id = 0;
            if (in.read(reinterpret_cast<char*>(&id), sizeof(id)) && id != 0) { return id; }
        }

        std::random_device rd;
        std::mt19937_64 rng(rd());
        uint64_t id = rng();
        if (id == 0) { id = 1; }

        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        if (!out.write(reinterpret_cast<const char*>(&id), sizeof(id))) {
            throw std::runtime_error("AkkEngine: failed to persist node id: " + path.string());
        }
        return id;
    }

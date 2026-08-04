/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/vlog/detail/VersionLogFormat.hpp
namespace {
    static constexpr uint32_t AKVLOG_V5_MAGIC = 0x35564B41u; // "AKV5"
    static constexpr uint16_t AKVLOG_V5_VERSION = 0x0001u;
    static constexpr uint32_t AKVLOG_INDEX_MAGIC = 0x49564B41u; // "AKVI"
    static constexpr uint16_t AKVLOG_INDEX_VERSION = 0x0002u;
    static constexpr uint32_t AKVLOG_TAIL_MAGIC = 0x54564B41u; // "AKVT"
    static constexpr uint16_t AKVLOG_TAIL_VERSION = 0x0001u;
    static constexpr uint32_t INDEX_BLOOM_BITS_PER_KEY = 10;
    static constexpr uint32_t INDEX_BLOOM_HASHES = 7;

    #pragma pack(push, 1)
    struct AkvlogV5FileHeader {
        uint32_t magic;
        uint16_t version;
        uint8_t syncModeHint;
        uint8_t reserved0;
        uint64_t createdNs;
        uint64_t reserved1;
        uint32_t crc32c;
        uint32_t reserved2;
    };
    #pragma pack(pop)

    #pragma pack(push, 1)
    struct AkvlogV5EntryHeader {
        uint32_t entryLen;
        uint64_t seq;
        uint64_t sourceNodeId;
        uint64_t timestampNs;
        uint8_t flags;
        uint64_t keyFp64;
        uint16_t keyLen;
        uint32_t valueLen;
    };
    #pragma pack(pop)

    #pragma pack(push, 1)
    struct AkvlogIndexFileHeader {
        uint32_t magic;
        uint16_t version;
        uint16_t bloomHashCount;
        uint64_t logBytes;
        uint64_t keyCount;
        uint64_t versionCount;
        uint32_t bloomBitCount;
        uint32_t payloadCrc32c;
        uint32_t crc32c;
    };
    #pragma pack(pop)

    #pragma pack(push, 1)
    struct AkvlogIndexKeyRecord {
        uint64_t keyFp64;
        uint64_t firstVersion;
        uint32_t versionCount;
        uint32_t reserved;
    };
    #pragma pack(pop)

    #pragma pack(push, 1)
    struct AkvlogIndexVersionRecord {
        uint64_t seq;
        uint64_t offset;
    };
    #pragma pack(pop)

    #pragma pack(push, 1)
    struct AkvlogTailFile {
        uint32_t magic;
        uint16_t version;
        uint16_t reserved0;
        uint64_t committedBytes;
        uint32_t crc32c;
        uint32_t reserved1;
    };
    #pragma pack(pop)

    static_assert(sizeof(AkvlogV5FileHeader) == 32);
    static_assert(sizeof(AkvlogV5EntryHeader) == 43);
    static_assert(sizeof(AkvlogIndexFileHeader) == 44);
    static_assert(sizeof(AkvlogIndexKeyRecord) == 24);
    static_assert(sizeof(AkvlogIndexVersionRecord) == 16);
    static_assert(sizeof(AkvlogTailFile) == 24);

    static constexpr size_t FILE_HDR_SIZE = 32;
    static constexpr size_t ENTRY_HDR_SIZE = 43;
    static constexpr size_t INDEX_FILE_HDR_SIZE = 44;
    static constexpr size_t INDEX_KEY_RECORD_SIZE = 24;
    static constexpr size_t INDEX_VERSION_RECORD_SIZE = 16;
    static constexpr size_t TAIL_FILE_SIZE = 24;
    static constexpr size_t CRC_SIZE = sizeof(uint32_t);
    static constexpr size_t MIN_ENTRY_SIZE = ENTRY_HDR_SIZE + CRC_SIZE;
    static constexpr size_t MAX_ENTRY_SIZE = 32u * 1024u * 1024u;
    static constexpr size_t ZSTD_VALUE_PREFIX_SIZE = sizeof(uint32_t);

    [[nodiscard]] static uint64_t nowNsFallback() noexcept {
        const auto now = std::chrono::system_clock::now().time_since_epoch();
        return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
    }

    static void appendU16Le(std::vector<uint8_t>& out, uint16_t value) {
        out.push_back(static_cast<uint8_t>(value & 0xffu));
        out.push_back(static_cast<uint8_t>((value >> 8u) & 0xffu));
    }

    static void appendU32Le(std::vector<uint8_t>& out, uint32_t value) {
        for (uint32_t shift = 0; shift < 32u; shift += 8u) { out.push_back(static_cast<uint8_t>((value >> shift) & 0xffu)); }
    }

    static void appendU64Le(std::vector<uint8_t>& out, uint64_t value) {
        for (uint32_t shift = 0; shift < 64u; shift += 8u) { out.push_back(static_cast<uint8_t>((value >> shift) & 0xffu)); }
    }

    [[nodiscard]] static uint16_t readU16Le(const uint8_t* data) noexcept {
        return static_cast<uint16_t>(data[0]) | static_cast<uint16_t>(static_cast<uint16_t>(data[1]) << 8u);
    }

    [[nodiscard]] static uint32_t readU32Le(const uint8_t* data) noexcept {
        uint32_t out = 0;
        for (uint32_t i = 0; i < 4u; ++i) { out |= static_cast<uint32_t>(data[i]) << (i * 8u); }
        return out;
    }

    [[nodiscard]] static uint64_t readU64Le(const uint8_t* data) noexcept {
        uint64_t out = 0;
        for (uint32_t i = 0; i < 8u; ++i) { out |= static_cast<uint64_t>(data[i]) << (i * 8u); }
        return out;
    }

    static void writeU32LeAt(std::vector<uint8_t>& out, size_t offset, uint32_t value) {
        out[offset] = static_cast<uint8_t>(value & 0xffu);
        out[offset + 1u] = static_cast<uint8_t>((value >> 8u) & 0xffu);
        out[offset + 2u] = static_cast<uint8_t>((value >> 16u) & 0xffu);
        out[offset + 3u] = static_cast<uint8_t>((value >> 24u) & 0xffu);
    }

    [[nodiscard]] static std::vector<uint8_t> encodeFileHeader(AkvlogV5FileHeader header) {
        std::vector<uint8_t> out;
        out.reserve(FILE_HDR_SIZE);
        appendU32Le(out, header.magic);
        appendU16Le(out, header.version);
        out.push_back(header.syncModeHint);
        out.push_back(header.reserved0);
        appendU64Le(out, header.createdNs);
        appendU64Le(out, header.reserved1);
        appendU32Le(out, header.crc32c);
        appendU32Le(out, header.reserved2);
        return out;
    }

    [[nodiscard]] static AkvlogV5FileHeader decodeFileHeader(const uint8_t* data) noexcept {
        AkvlogV5FileHeader header{};
        header.magic = readU32Le(data);
        header.version = readU16Le(data + 4u);
        header.syncModeHint = data[6];
        header.reserved0 = data[7];
        header.createdNs = readU64Le(data + 8u);
        header.reserved1 = readU64Le(data + 16u);
        header.crc32c = readU32Le(data + 24u);
        header.reserved2 = readU32Le(data + 28u);
        return header;
    }

    [[nodiscard]] static std::vector<uint8_t> encodeEntryHeader(const AkvlogV5EntryHeader& header) {
        std::vector<uint8_t> out;
        out.reserve(ENTRY_HDR_SIZE);
        appendU32Le(out, header.entryLen);
        appendU64Le(out, header.seq);
        appendU64Le(out, header.sourceNodeId);
        appendU64Le(out, header.timestampNs);
        out.push_back(header.flags);
        appendU64Le(out, header.keyFp64);
        appendU16Le(out, header.keyLen);
        appendU32Le(out, header.valueLen);
        return out;
    }

    [[nodiscard]] static AkvlogV5EntryHeader decodeEntryHeader(const uint8_t* data) noexcept {
        AkvlogV5EntryHeader header{};
        header.entryLen = readU32Le(data);
        header.seq = readU64Le(data + 4u);
        header.sourceNodeId = readU64Le(data + 12u);
        header.timestampNs = readU64Le(data + 20u);
        header.flags = data[28];
        header.keyFp64 = readU64Le(data + 29u);
        header.keyLen = readU16Le(data + 37u);
        header.valueLen = readU32Le(data + 39u);
        return header;
    }

    [[nodiscard]] static std::vector<uint8_t> encodeIndexHeader(AkvlogIndexFileHeader header) {
        std::vector<uint8_t> out;
        out.reserve(INDEX_FILE_HDR_SIZE);
        appendU32Le(out, header.magic);
        appendU16Le(out, header.version);
        appendU16Le(out, header.bloomHashCount);
        appendU64Le(out, header.logBytes);
        appendU64Le(out, header.keyCount);
        appendU64Le(out, header.versionCount);
        appendU32Le(out, header.bloomBitCount);
        appendU32Le(out, header.payloadCrc32c);
        appendU32Le(out, header.crc32c);
        return out;
    }

    [[nodiscard]] static AkvlogIndexFileHeader decodeIndexHeader(const uint8_t* data) noexcept {
        AkvlogIndexFileHeader header{};
        header.magic = readU32Le(data);
        header.version = readU16Le(data + 4u);
        header.bloomHashCount = readU16Le(data + 6u);
        header.logBytes = readU64Le(data + 8u);
        header.keyCount = readU64Le(data + 16u);
        header.versionCount = readU64Le(data + 24u);
        header.bloomBitCount = readU32Le(data + 32u);
        header.payloadCrc32c = readU32Le(data + 36u);
        header.crc32c = readU32Le(data + 40u);
        return header;
    }

    [[nodiscard]] static std::vector<uint8_t> encodeIndexKeyRecord(const AkvlogIndexKeyRecord& record) {
        std::vector<uint8_t> out;
        out.reserve(INDEX_KEY_RECORD_SIZE);
        appendU64Le(out, record.keyFp64);
        appendU64Le(out, record.firstVersion);
        appendU32Le(out, record.versionCount);
        appendU32Le(out, record.reserved);
        return out;
    }

    [[nodiscard]] static AkvlogIndexKeyRecord decodeIndexKeyRecord(const uint8_t* data) noexcept {
        AkvlogIndexKeyRecord record{};
        record.keyFp64 = readU64Le(data);
        record.firstVersion = readU64Le(data + 8u);
        record.versionCount = readU32Le(data + 16u);
        record.reserved = readU32Le(data + 20u);
        return record;
    }

    [[nodiscard]] static std::vector<uint8_t> encodeIndexVersionRecord(const AkvlogIndexVersionRecord& record) {
        std::vector<uint8_t> out;
        out.reserve(INDEX_VERSION_RECORD_SIZE);
        appendU64Le(out, record.seq);
        appendU64Le(out, record.offset);
        return out;
    }

    [[nodiscard]] static AkvlogIndexVersionRecord decodeIndexVersionRecord(const uint8_t* data) noexcept {
        AkvlogIndexVersionRecord record{};
        record.seq = readU64Le(data);
        record.offset = readU64Le(data + 8u);
        return record;
    }

    [[nodiscard]] static std::vector<uint8_t> encodeTailFile(AkvlogTailFile tail) {
        std::vector<uint8_t> out;
        out.reserve(TAIL_FILE_SIZE);
        appendU32Le(out, tail.magic);
        appendU16Le(out, tail.version);
        appendU16Le(out, tail.reserved0);
        appendU64Le(out, tail.committedBytes);
        appendU32Le(out, tail.crc32c);
        appendU32Le(out, tail.reserved1);
        return out;
    }

    [[nodiscard]] static AkvlogTailFile decodeTailFile(const uint8_t* data) noexcept {
        AkvlogTailFile tail{};
        tail.magic = readU32Le(data);
        tail.version = readU16Le(data + 4u);
        tail.reserved0 = readU16Le(data + 6u);
        tail.committedBytes = readU64Le(data + 8u);
        tail.crc32c = readU32Le(data + 16u);
        tail.reserved1 = readU32Le(data + 20u);
        return tail;
    }

    static void flushChecked(FILE* file) {
        if (std::fflush(file) != 0) { throw std::system_error(errno, std::generic_category(), "VersionLog: fflush failed"); }
    }

    static void fdatasyncChecked(FILE* f) {
        #ifdef _WIN32
        if (_commit(_fileno(f)) != 0) { throw std::system_error(errno, std::generic_category(), "VersionLog: fdatasync failed"); }
        #else
        if (::fdatasync(::fileno(f)) != 0) { throw std::system_error(errno, std::generic_category(), "VersionLog: fdatasync failed"); }
        #endif
    }

    static void closeChecked(FILE* file) {
        if (std::fclose(file) != 0) { throw std::system_error(errno, std::generic_category(), "VersionLog: fclose failed"); }
    }

    #ifndef _WIN32
    static void syncParentDirectory(const fs::path& path) {
        const fs::path parent = path.parent_path().empty() ? fs::path{"."} : path.parent_path();
        const int fd = ::open(parent.string().c_str(), O_RDONLY | O_DIRECTORY);
        if (fd < 0) { throw std::system_error(errno, std::generic_category(), "VersionLog: cannot open parent directory"); }
        if (::fsync(fd) != 0) {
            const int savedErrno = errno;
            ::close(fd);
            throw std::system_error(savedErrno, std::generic_category(), "VersionLog: parent directory fsync failed");
        }
        if (::close(fd) != 0) { throw std::system_error(errno, std::generic_category(), "VersionLog: parent directory close failed"); }
    }
    #endif

    static void replaceFileAtomically(const fs::path& temporary, const fs::path& path) {
        #ifdef _WIN32
        if (!MoveFileExW(temporary.wstring().c_str(), path.wstring().c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
            throw std::system_error(static_cast<int>(GetLastError()), std::system_category(), "VersionLog: atomic file replace failed");
        }
        #else
        std::error_code error;
        fs::rename(temporary, path, error);
        if (error) { throw std::system_error(error, "VersionLog: atomic file replace failed"); }
        syncParentDirectory(path);
        #endif
    }

    [[nodiscard]] static uint64_t fileOffset(FILE* file) {
        #ifdef _WIN32
        const auto offset = _ftelli64(file);
        #else
        const auto offset = ftello(file);
        #endif
        if (offset < 0) { throw std::runtime_error("VersionLog: cannot determine file offset"); }
        return static_cast<uint64_t>(offset);
    }

    static void seekFile(FILE* file, uint64_t offset) {
        #ifdef _WIN32
        if (_fseeki64(file, static_cast<__int64>(offset), SEEK_SET) != 0) {
        #else
        if (fseeko(file, static_cast<off_t>(offset), SEEK_SET) != 0) {
            #endif
            throw std::runtime_error("VersionLog: failed to seek file");
        }
    }

    [[nodiscard]] static std::string fileContext(
        const fs::path& path,
        std::optional<uint64_t> offset = std::nullopt,
        std::optional<uint64_t> seq = std::nullopt
    ) {
        std::string out = " path=" + path.string();
        if (offset.has_value()) { out += " offset=" + std::to_string(*offset); }
        if (seq.has_value()) { out += " seq=" + std::to_string(*seq); }
        return out;
    }

    [[noreturn]] static void throwVLogError(
        const std::string& message,
        const fs::path& path,
        std::optional<uint64_t> offset = std::nullopt,
        std::optional<uint64_t> seq = std::nullopt
    ) { throw std::runtime_error("VersionLog: " + message + fileContext(path, offset, seq)); }

    [[nodiscard]] static uint64_t mixIndexHash(uint64_t value) noexcept {
        value ^= value >> 30u;
        value *= 0xbf58476d1ce4e5b9ULL;
        value ^= value >> 27u;
        value *= 0x94d049bb133111ebULL;
        return value ^ (value >> 31u);
    }
} // namespace

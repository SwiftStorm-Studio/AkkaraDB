/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/vlog/detail/VersionLogScan.hpp
void insertSorted(const std::string& key, VersionEntry ve) {
    auto& versions = residentIndex_[key];
    if (versions.empty() || versions.back().seq <= ve.seq) {
        versions.push_back(std::move(ve));
        return;
    }

    const auto pos = std::lower_bound(
        versions.begin(),
        versions.end(),
        ve.seq,
        [](const VersionEntry& e, uint64_t seq) { return e.seq < seq; }
    );
    versions.insert(pos, std::move(ve));
}

struct ScanSummary {
    uint64_t firstSeq = 0;
    uint64_t maxSeq = 0;
    uint64_t entryCount = 0;
    uint64_t rollbackCount = 0;
    uint64_t durableBytes = 0;
    bool stoppedAtTrailingEntry = false;
    bool hasEntries = false;
};

[[nodiscard]] FILE* openReadFile(const fs::path& path) const {
    #ifdef _WIN32
    return _wfopen(path.wstring().c_str(), L"rb");
    #else
    return fopen(path.string().c_str(), "rb");
    #endif
}

template <typename Visitor>
[[nodiscard]] ScanSummary scanFile(
    FILE* rf,
    const fs::path& path,
    bool allowTrailingEntry,
    bool allowTrailingHeader,
    bool allowTrailingCorruption,
    Visitor&& visitor,
    uint64_t maxBytes = std::numeric_limits<uint64_t>::max()
) const {
    ScanSummary summary;
    if (maxBytes < FILE_HDR_SIZE) { throwVLogError("durable tail precedes file header", path, maxBytes); }
    std::array<uint8_t, FILE_HDR_SIZE> encodedFileHdr{};
    const size_t headerRead = fread(encodedFileHdr.data(), 1, encodedFileHdr.size(), rf);
    if (headerRead == 0 && feof(rf)) { return summary; }
    if (headerRead != encodedFileHdr.size()) {
        if (allowTrailingHeader && feof(rf)) {
            summary.stoppedAtTrailingEntry = true;
            return summary;
        }
        throwVLogError("truncated file header", path, 0);
    }

    AkvlogV5FileHeader fileHdr = decodeFileHeader(encodedFileHdr.data());
    const uint32_t storedHeaderCrc = fileHdr.crc32c;
    fileHdr.crc32c = 0;
    encodedFileHdr[24] = 0;
    encodedFileHdr[25] = 0;
    encodedFileHdr[26] = 0;
    encodedFileHdr[27] = 0;
    const uint32_t computedHeaderCrc = cpu::CRC32C(reinterpret_cast<const std::byte*>(encodedFileHdr.data()), encodedFileHdr.size());
    if (fileHdr.magic != AKVLOG_V5_MAGIC || fileHdr.version != AKVLOG_V5_VERSION || storedHeaderCrc != computedHeaderCrc) {
        throwVLogError("corrupt file header", path, 0);
    }
    summary.durableBytes += FILE_HDR_SIZE;

    std::vector<uint8_t> buf;
    while (true) {
        const uint64_t entryOffset = fileOffset(rf);
        if (entryOffset == maxBytes) { break; }
        if (entryOffset > maxBytes || maxBytes - entryOffset < sizeof(uint32_t)) {
            if (allowTrailingEntry) {
                summary.stoppedAtTrailingEntry = true;
                break;
            }
            throwVLogError("durable tail splits entry length", path, entryOffset);
        }
        std::array<uint8_t, sizeof(uint32_t)> encodedEntryLen{};
        const size_t prefixRead = fread(encodedEntryLen.data(), 1, encodedEntryLen.size(), rf);
        if (prefixRead == 0 && feof(rf)) { break; }
        if (prefixRead != encodedEntryLen.size()) {
            if (allowTrailingEntry && feof(rf)) {
                summary.stoppedAtTrailingEntry = true;
                break;
            }
            throwVLogError("truncated entry length", path, entryOffset);
        }
        const uint32_t entryLen = readU32Le(encodedEntryLen.data());
        if (entryLen < MIN_ENTRY_SIZE || entryLen > MAX_ENTRY_SIZE) {
            if (allowTrailingCorruption) {
                summary.stoppedAtTrailingEntry = true;
                break;
            }
            throwVLogError("corrupt entry length", path, entryOffset);
        }
        if (static_cast<uint64_t>(entryLen) > maxBytes - entryOffset) {
            if (allowTrailingEntry) {
                summary.stoppedAtTrailingEntry = true;
                break;
            }
            throwVLogError("durable tail splits entry", path, entryOffset);
        }

        buf.resize(entryLen);
        std::memcpy(buf.data(), encodedEntryLen.data(), encodedEntryLen.size());

        const size_t rest = entryLen - sizeof(entryLen);
        if (fread(buf.data() + sizeof(entryLen), 1, rest, rf) != rest) {
            if (allowTrailingEntry && feof(rf)) {
                summary.stoppedAtTrailingEntry = true;
                break;
            }
            throwVLogError("truncated entry", path, entryOffset);
        }

        const uint32_t storedEntryCrc = readU32Le(buf.data() + entryLen - CRC_SIZE);
        std::memset(buf.data() + entryLen - CRC_SIZE, 0, CRC_SIZE);
        const uint32_t computedEntryCrc = cpu::CRC32C(reinterpret_cast<const std::byte*>(buf.data()), entryLen - CRC_SIZE);
        const auto ehdr = decodeEntryHeader(buf.data());
        if (storedEntryCrc != computedEntryCrc) {
            if (allowTrailingCorruption) {
                summary.stoppedAtTrailingEntry = true;
                break;
            }
            throwVLogError("entry CRC mismatch", path, entryOffset, ehdr.seq);
        }

        if (buf.size() < ENTRY_HDR_SIZE) { throwVLogError("corrupt entry header", path, entryOffset); }
        const size_t expectedSize = ENTRY_HDR_SIZE + ehdr.keyLen + ehdr.valueLen + CRC_SIZE;
        if (expectedSize != entryLen) { throwVLogError("corrupt entry payload", path, entryOffset, ehdr.seq); }

        const uint8_t* p = buf.data() + ENTRY_HDR_SIZE;
        const std::string_view key(reinterpret_cast<const char*>(p), ehdr.keyLen);
        p += ehdr.keyLen;
        const std::span<const uint8_t> storedValue{p, ehdr.valueLen};
        std::vector<uint8_t> decodedValue;
        std::span<const uint8_t> value = storedValue;
        if ((ehdr.flags & VLOG_FLAG_ZSTD) != 0) {
            if (storedValue.size() <= ZSTD_VALUE_PREFIX_SIZE) {
                throwVLogError("invalid compressed value record", path, entryOffset, ehdr.seq);
            }
            const uint32_t rawSize = readU32Le(storedValue.data());
            if (rawSize == 0 || rawSize > MAX_ENTRY_SIZE) { throwVLogError("invalid compressed value size", path, entryOffset, ehdr.seq); }
            decodedValue.resize(rawSize);
            const size_t decodedSize = ZSTD_decompress(
                decodedValue.data(),
                decodedValue.size(),
                storedValue.data() + ZSTD_VALUE_PREFIX_SIZE,
                storedValue.size() - ZSTD_VALUE_PREFIX_SIZE
            );
            if (ZSTD_isError(decodedSize) || decodedSize != rawSize) {
                throwVLogError("corrupt Zstd value payload", path, entryOffset, ehdr.seq);
            }
            value = std::span<const uint8_t>{decodedValue};
        }
        auto logicalHeader = ehdr;
        logicalHeader.flags &= static_cast<uint8_t>(~VLOG_FLAG_ZSTD);
        if constexpr (std::is_invocable_v<Visitor&, std::string_view, const AkvlogV5EntryHeader&, std::span<const uint8_t>, uint64_t>) {
            visitor(key, logicalHeader, value, entryOffset);
        }
        else { visitor(key, logicalHeader, value); }
        if (!summary.hasEntries) {
            summary.firstSeq = ehdr.seq;
            summary.maxSeq = ehdr.seq;
            summary.hasEntries = true;
        }
        else {
            summary.firstSeq = std::min(summary.firstSeq, ehdr.seq);
            summary.maxSeq = std::max(summary.maxSeq, ehdr.seq);
        }
        ++summary.entryCount;
        if ((ehdr.flags & VLOG_FLAG_ROLLBACK) != 0) { ++summary.rollbackCount; }
        summary.durableBytes += entryLen;
    }
    return summary;
}

static void mergeScanSummary(ScanSummary& total, const ScanSummary& part) {
    if (part.hasEntries) {
        if (!total.hasEntries) {
            total.firstSeq = part.firstSeq;
            total.maxSeq = part.maxSeq;
            total.hasEntries = true;
        }
        else {
            total.firstSeq = std::min(total.firstSeq, part.firstSeq);
            total.maxSeq = std::max(total.maxSeq, part.maxSeq);
        }
    }
    total.entryCount += part.entryCount;
    total.rollbackCount += part.rollbackCount;
    total.durableBytes += part.durableBytes;
}

template <typename Visitor>
[[nodiscard]] ScanSummary scanSegment(
    const fs::path& path,
    bool allowTrailingEntry,
    Visitor&& visitor,
    bool allowTrailingCorruption = false,
    bool allowTrailingHeader = false
) const {
    FILE* rf = openReadFile(path);
    if (!rf) { throw std::runtime_error("VersionLog: cannot open segment for reading: " + path.string()); }
    try {
        uint64_t maxBytes = std::numeric_limits<uint64_t>::max();
        if (const auto tail = readTailFile(path); tail.has_value()) {
            std::error_code error;
            const uint64_t actualBytes = fs::file_size(path, error);
            if (error) { throwVLogError("cannot stat segment for durable tail validation: " + error.message(), path); }
            if (*tail > actualBytes) { throwVLogError("invalid durable tail length", path, *tail); }
            maxBytes = *tail;
        }
        auto summary = scanFile(
            rf,
            path,
            allowTrailingEntry,
            allowTrailingHeader,
            allowTrailingCorruption,
            std::forward<Visitor>(visitor),
            maxBytes
        );
        fclose(rf);
        return summary;
    }
    catch (...) {
        fclose(rf);
        throw;
    }
}

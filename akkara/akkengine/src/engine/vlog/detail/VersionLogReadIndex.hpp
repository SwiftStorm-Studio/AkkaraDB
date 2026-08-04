/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/vlog/detail/VersionLogReadIndex.hpp
class IndexUnavailable final : public std::runtime_error {
    public:
        using std::runtime_error::runtime_error;
};

struct ParsedEntry {
    std::string key;
    VersionEntry entry;
};

[[nodiscard]] ParsedEntry readEntryAt(FILE* file, const fs::path& path, uint64_t offset) const {
    (void)path;
    seekFile(file, offset);
    std::array<uint8_t, sizeof(uint32_t)> encodedEntryLen{};
    if (fread(encodedEntryLen.data(), 1, encodedEntryLen.size(), file) != encodedEntryLen.size()) {
        throw IndexUnavailable("VersionLog: truncated indexed entry length");
    }
    const uint32_t entryLen = readU32Le(encodedEntryLen.data());
    if (entryLen < MIN_ENTRY_SIZE || entryLen > MAX_ENTRY_SIZE) { throw IndexUnavailable("VersionLog: corrupt indexed entry length"); }

    std::vector<uint8_t> buffer(entryLen);
    std::memcpy(buffer.data(), encodedEntryLen.data(), encodedEntryLen.size());
    const size_t remaining = entryLen - sizeof(entryLen);
    if (fread(buffer.data() + sizeof(entryLen), 1, remaining, file) != remaining) {
        throw IndexUnavailable("VersionLog: truncated indexed entry");
    }

    const uint32_t storedCrc = readU32Le(buffer.data() + entryLen - CRC_SIZE);
    std::memset(buffer.data() + entryLen - CRC_SIZE, 0, CRC_SIZE);
    const uint32_t computedCrc = cpu::CRC32C(reinterpret_cast<const std::byte*>(buffer.data()), entryLen - CRC_SIZE);
    const auto header = decodeEntryHeader(buffer.data());
    if (storedCrc != computedCrc) { throw IndexUnavailable("VersionLog: indexed entry CRC mismatch"); }

    const size_t expectedSize = ENTRY_HDR_SIZE + header.keyLen + header.valueLen + CRC_SIZE;
    if (expectedSize != entryLen) { throw IndexUnavailable("VersionLog: corrupt indexed entry payload"); }

    const uint8_t* data = buffer.data() + ENTRY_HDR_SIZE;
    ParsedEntry parsed;
    parsed.key.assign(reinterpret_cast<const char*>(data), header.keyLen);
    data += header.keyLen;
    const std::span<const uint8_t> storedValue{data, header.valueLen};
    std::span<const uint8_t> value = storedValue;
    std::vector<uint8_t> decodedValue;
    if ((header.flags & VLOG_FLAG_ZSTD) != 0) {
        if (storedValue.size() <= ZSTD_VALUE_PREFIX_SIZE) {
            throw std::runtime_error("VersionLog: invalid indexed compressed value record");
        }
        const uint32_t rawSize = readU32Le(storedValue.data());
        if (rawSize == 0 || rawSize > MAX_ENTRY_SIZE) { throw std::runtime_error("VersionLog: invalid indexed compressed value size"); }
        decodedValue.resize(rawSize);
        const size_t decodedSize = ZSTD_decompress(
            decodedValue.data(),
            decodedValue.size(),
            storedValue.data() + ZSTD_VALUE_PREFIX_SIZE,
            storedValue.size() - ZSTD_VALUE_PREFIX_SIZE
        );
        if (ZSTD_isError(decodedSize) || decodedSize != rawSize) {
            throw std::runtime_error("VersionLog: corrupt indexed Zstd value payload");
        }
        value = std::span<const uint8_t>{decodedValue};
    }

    parsed.entry.seq = header.seq;
    parsed.entry.sourceNodeId = header.sourceNodeId;
    parsed.entry.timestampNs = header.timestampNs;
    parsed.entry.flags = header.flags & static_cast<uint8_t>(~VLOG_FLAG_ZSTD);
    parsed.entry.value.assign(value.begin(), value.end());
    return parsed;
}

[[nodiscard]] static bool indexLayoutIsValid(const fs::path& path, const AkvlogIndexFileHeader& header) {
    if (header.magic != AKVLOG_INDEX_MAGIC || header.version != AKVLOG_INDEX_VERSION || header.bloomHashCount != INDEX_BLOOM_HASHES ||
        header.bloomBitCount == 0) { return false; }
    AkvlogIndexFileHeader checksum = header;
    const uint32_t storedCrc = checksum.crc32c;
    checksum.crc32c = 0;
    const auto encodedChecksum = encodeIndexHeader(checksum);
    if (storedCrc != cpu::CRC32C(reinterpret_cast<const std::byte*>(encodedChecksum.data()), encodedChecksum.size())) { return false; }

    const uint64_t bloomBytes = (static_cast<uint64_t>(header.bloomBitCount) + 7u) / 8u;
    if (header.keyCount > (std::numeric_limits<uint64_t>::max() - INDEX_FILE_HDR_SIZE - bloomBytes) / INDEX_KEY_RECORD_SIZE) {
        return false;
    }
    uint64_t expectedBytes = INDEX_FILE_HDR_SIZE + bloomBytes + header.keyCount * INDEX_KEY_RECORD_SIZE;
    if (header.versionCount > (std::numeric_limits<uint64_t>::max() - expectedBytes) / INDEX_VERSION_RECORD_SIZE) { return false; }
    expectedBytes += header.versionCount * INDEX_VERSION_RECORD_SIZE;
    std::error_code error;
    const uint64_t actualBytes = fs::file_size(path, error);
    return !error && actualBytes == expectedBytes;
}

[[nodiscard]] bool sidecarIndexVersions(const SegmentInfo& segment, std::string_view key, std::vector<IndexVersion>& out) const {
    const fs::path path = segmentIndexPath(segment.path);
    FILE* indexFile = openReadFile(path);
    if (!indexFile) {
        sidecarFallbackCount_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    try {
        std::vector<uint8_t> encodedHeader(INDEX_FILE_HDR_SIZE);
        if (fread(encodedHeader.data(), 1, encodedHeader.size(), indexFile) != encodedHeader.size()) {
            fclose(indexFile);
            sidecarFallbackCount_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        AkvlogIndexFileHeader header = decodeIndexHeader(encodedHeader.data());
        if (header.logBytes != segment.bytes || !indexLayoutIsValid(path, header) || !verifyIndexPayload(indexFile, path, header)) {
            fclose(indexFile);
            sidecarFallbackCount_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        const uint64_t fingerprint = key.empty() ? 0ULL : core::computeKeyFp64(reinterpret_cast<const uint8_t*>(key.data()), key.size());
        if (!bloomMayContain(indexFile, header, fingerprint)) {
            fclose(indexFile);
            return true;
        }

        const uint64_t bloomBytes = (static_cast<uint64_t>(header.bloomBitCount) + 7u) / 8u;
        const uint64_t directoryOffset = INDEX_FILE_HDR_SIZE + bloomBytes;
        uint64_t first = 0;
        uint64_t last = header.keyCount;
        while (first < last) {
            const uint64_t middle = first + (last - first) / 2u;
            std::array<uint8_t, INDEX_KEY_RECORD_SIZE> encodedCandidate{};
            seekFile(indexFile, directoryOffset + middle * INDEX_KEY_RECORD_SIZE);
            if (fread(encodedCandidate.data(), 1, encodedCandidate.size(), indexFile) != encodedCandidate.size()) {
                throw IndexUnavailable("VersionLog: truncated segment index directory");
            }
            const auto candidate = decodeIndexKeyRecord(encodedCandidate.data());
            if (candidate.keyFp64 < fingerprint) { first = middle + 1u; }
            else { last = middle; }
        }

        if (first == header.keyCount) {
            fclose(indexFile);
            return true;
        }

        FILE* logFile = openReadFile(segment.path);
        if (!logFile) { throw std::runtime_error("VersionLog: cannot open indexed segment: " + segment.path.string()); }
        try {
            const uint64_t versionOffset = directoryOffset + header.keyCount * INDEX_KEY_RECORD_SIZE;
            for (uint64_t directory = first; directory < header.keyCount; ++directory) {
                std::array<uint8_t, INDEX_KEY_RECORD_SIZE> encodedCandidate{};
                seekFile(indexFile, directoryOffset + directory * INDEX_KEY_RECORD_SIZE);
                if (fread(encodedCandidate.data(), 1, encodedCandidate.size(), indexFile) != encodedCandidate.size()) {
                    throw IndexUnavailable("VersionLog: truncated segment index directory");
                }
                const auto candidate = decodeIndexKeyRecord(encodedCandidate.data());
                if (candidate.keyFp64 != fingerprint) { break; }
                if (candidate.versionCount == 0 || candidate.firstVersion > header.versionCount || candidate.versionCount > header.
                    versionCount - candidate.firstVersion) { throw IndexUnavailable("VersionLog: invalid segment index version range"); }
                std::array<uint8_t, INDEX_VERSION_RECORD_SIZE> encodedFirstVersion{};
                seekFile(indexFile, versionOffset + candidate.firstVersion * INDEX_VERSION_RECORD_SIZE);
                if (fread(encodedFirstVersion.data(), 1, encodedFirstVersion.size(), indexFile) != encodedFirstVersion.size()) {
                    throw IndexUnavailable("VersionLog: truncated segment index version record");
                }
                const auto firstVersion = decodeIndexVersionRecord(encodedFirstVersion.data());
                if (readEntryAt(logFile, segment.path, firstVersion.offset).key != key) { continue; }

                std::vector<uint8_t> encodedVersions(static_cast<size_t>(candidate.versionCount) * INDEX_VERSION_RECORD_SIZE);
                seekFile(indexFile, versionOffset + candidate.firstVersion * INDEX_VERSION_RECORD_SIZE);
                if (fread(encodedVersions.data(), 1, encodedVersions.size(), indexFile) != encodedVersions.size()) {
                    throw IndexUnavailable("VersionLog: truncated segment index versions");
                }
                out.reserve(out.size() + candidate.versionCount);
                for (uint32_t i = 0; i < candidate.versionCount; ++i) {
                    const auto version = decodeIndexVersionRecord(
                        encodedVersions.data() + static_cast<size_t>(i) * INDEX_VERSION_RECORD_SIZE
                    );
                    out.push_back(IndexVersion{version.seq, version.offset});
                }
                fclose(logFile);
                fclose(indexFile);
                return true;
            }
            fclose(logFile);
        }
        catch (...) {
            fclose(logFile);
            throw;
        }
        fclose(indexFile);
        return true;
    }
    catch (const IndexUnavailable&) {
        fclose(indexFile);
        sidecarFallbackCount_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    catch (...) {
        fclose(indexFile);
        sidecarFallbackCount_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
}

[[nodiscard]] bool indexVersionsForSegment(const SegmentInfo& segment, std::string_view key, std::vector<IndexVersion>& out) const {
    return activeIndexVersions(segment.id, key, out) || sidecarIndexVersions(segment, key, out);
}

[[nodiscard]] std::vector<VersionEntry> readIndexedEntries(
    const SegmentInfo& segment,
    std::string_view key,
    std::span<const IndexVersion> versions,
    uint64_t visibleSeq
) const {
    if (versions.empty()) { return {}; }
    FILE* file = openReadFile(segment.path);
    if (!file) { throw std::runtime_error("VersionLog: cannot open indexed segment: " + segment.path.string()); }
    try {
        std::vector<VersionEntry> entries;
        entries.reserve(versions.size());
        for (const auto& version : versions) {
            if (version.seq > visibleSeq) { continue; }
            auto parsed = readEntryAt(file, segment.path, version.offset);
            if (parsed.key != key || parsed.entry.seq != version.seq) { throw IndexUnavailable("VersionLog: stale segment index entry"); }
            entries.push_back(std::move(parsed.entry));
        }
        fclose(file);
        return entries;
    }
    catch (...) {
        fclose(file);
        throw;
    }
}

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/vlog/detail/VersionLogFiles.hpp
static void writeFileHeaderRaw(FILE* wf, VLogSyncMode syncModeHint) {
    AkvlogV5FileHeader hdr{};
    hdr.magic = AKVLOG_V5_MAGIC;
    hdr.version = AKVLOG_V5_VERSION;
    hdr.syncModeHint = static_cast<uint8_t>(syncModeHint);
    hdr.reserved0 = 0;
    hdr.createdNs = nowNsFallback();
    hdr.reserved1 = 0;
    hdr.crc32c = 0;
    hdr.reserved2 = 0;
    auto encoded = encodeFileHeader(hdr);
    hdr.crc32c = cpu::CRC32C(reinterpret_cast<const std::byte*>(encoded.data()), encoded.size());
    writeU32LeAt(encoded, 24u, hdr.crc32c);

    if (fwrite(encoded.data(), 1, encoded.size(), wf) != encoded.size()) { throw std::runtime_error("VersionLog: failed to write header"); }
    flushChecked(wf);
    fdatasyncChecked(wf);
}

void writeFileHeader(FILE* wf) {
    writeFileHeaderRaw(wf, opts_.syncMode);
    durableBytes_ += FILE_HDR_SIZE;
}

[[nodiscard]] fs::path segmentPath(uint64_t id) const {
    if (id == 0) { return logPath_; }
    const auto name = logPath_.stem().string() + "-seg-" + std::to_string(id) + logPath_.extension().string();
    return logPath_.parent_path() / name;
}

[[nodiscard]] static fs::path segmentIndexPath(const fs::path& segmentPath) {
    auto indexPath = segmentPath;
    indexPath.replace_extension(".akvidx");
    return indexPath;
}

[[nodiscard]] static fs::path segmentTailPath(const fs::path& segmentPath) {
    auto tailPath = segmentPath;
    tailPath.replace_extension(".akvtail");
    return tailPath;
}

static void writeTailFile(const fs::path& segmentPath, uint64_t committedBytes, bool sync) {
    AkvlogTailFile tail{};
    tail.magic = AKVLOG_TAIL_MAGIC;
    tail.version = AKVLOG_TAIL_VERSION;
    tail.committedBytes = committedBytes;
    tail.crc32c = 0;
    auto encoded = encodeTailFile(tail);
    tail.crc32c = cpu::CRC32C(reinterpret_cast<const std::byte*>(encoded.data()), encoded.size());
    writeU32LeAt(encoded, 16u, tail.crc32c);

    const auto path = segmentTailPath(segmentPath);
    auto temporary = path;
    temporary += ".tmp";
    FILE* file = nullptr;
    try {
        #ifdef _WIN32
        file = _wfopen(temporary.wstring().c_str(), L"wb");
        #else
        file = fopen(temporary.string().c_str(), "wb");
        #endif
        if (!file || fwrite(encoded.data(), 1, encoded.size(), file) != encoded.size()) {
            throwVLogError("failed to write durable tail", temporary);
        }
        flushChecked(file);
        if (sync) { fdatasyncChecked(file); }
        closeChecked(file);
        file = nullptr;
        replaceFileAtomically(temporary, path);
    }
    catch (...) {
        if (file) { fclose(file); }
        std::error_code ignored;
        fs::remove(temporary, ignored);
        throw;
    }
}

[[nodiscard]] static std::optional<uint64_t> readTailFile(const fs::path& segmentPath) {
    const auto path = segmentTailPath(segmentPath);
    std::error_code sizeError;
    const bool exists = fs::exists(path, sizeError);
    if (sizeError) { throwVLogError("cannot inspect durable tail: " + sizeError.message(), path); }
    if (!exists) { return std::nullopt; }
    const uint64_t bytes = fs::file_size(path, sizeError);
    if (sizeError) { throwVLogError("cannot stat durable tail: " + sizeError.message(), path); }
    if (bytes != TAIL_FILE_SIZE) { throwVLogError("invalid durable tail size", path, bytes); }
    #ifdef _WIN32
    FILE* file = _wfopen(path.wstring().c_str(), L"rb");
    #else
    FILE* file = fopen(path.string().c_str(), "rb");
    #endif
    if (!file) { throwVLogError("cannot open durable tail", path); }
    std::vector<uint8_t> encoded(TAIL_FILE_SIZE);
    const bool valid = fread(encoded.data(), 1, encoded.size(), file) == encoded.size();
    fclose(file);
    if (!valid) { throwVLogError("truncated durable tail", path); }
    AkvlogTailFile tail = decodeTailFile(encoded.data());
    const uint32_t stored = tail.crc32c;
    tail.crc32c = 0;
    writeU32LeAt(encoded, 16u, 0);
    if (tail.magic != AKVLOG_TAIL_MAGIC || tail.version != AKVLOG_TAIL_VERSION || stored != cpu::CRC32C(
        reinterpret_cast<const std::byte*>(encoded.data()),
        encoded.size()
    )) { throwVLogError("corrupt durable tail", path, tail.committedBytes); }
    return tail.committedBytes;
}

void markIndexPayloadValidated(const fs::path& path, uint32_t payloadCrc32c) const {
    std::error_code error;
    const uint64_t bytes = fs::file_size(path, error);
    if (error) { return; }
    const auto modified = fs::last_write_time(path, error);
    if (error) { return; }
    std::lock_guard lock{indexValidationMu_};
    validatedIndexPayloads_[path.string()] = ValidatedIndexPayload{payloadCrc32c, bytes, modified};
}

void forgetIndexPayloadValidation(const fs::path& path) const {
    std::lock_guard lock{indexValidationMu_};
    validatedIndexPayloads_.erase(path.string());
}

[[nodiscard]] bool verifyIndexPayload(FILE* file, const fs::path& path, const AkvlogIndexFileHeader& header) const {
    std::error_code error;
    const uint64_t totalBytes = fs::file_size(path, error);
    if (error || totalBytes < INDEX_FILE_HDR_SIZE || totalBytes - INDEX_FILE_HDR_SIZE > std::numeric_limits<size_t>::max()) {
        return false;
    }
    const auto modified = fs::last_write_time(path, error);
    if (error) { return false; }
    {
        std::lock_guard lock{indexValidationMu_};
        const auto it = validatedIndexPayloads_.find(path.string());
        if (it != validatedIndexPayloads_.end() && it->second.crc32c == header.payloadCrc32c && it->second.bytes == totalBytes && it->second
           .modified == modified) { return true; }
    }

    std::vector<uint8_t> payload(static_cast<size_t>(totalBytes - INDEX_FILE_HDR_SIZE));
    seekFile(file, INDEX_FILE_HDR_SIZE);
    if (!payload.empty() && fread(payload.data(), 1, payload.size(), file) != payload.size()) { return false; }
    if (cpu::CRC32C(reinterpret_cast<const std::byte*>(payload.data()), payload.size()) != header.payloadCrc32c) { return false; }
    markIndexPayloadValidated(path, header.payloadCrc32c);
    return true;
}

static void addBloomFingerprint(std::vector<uint8_t>& bloom, uint64_t bitCount, uint64_t fingerprint) {
    for (uint32_t hash = 0; hash < INDEX_BLOOM_HASHES; ++hash) {
        const uint64_t bit = mixIndexHash(fingerprint + 0x9e3779b97f4a7c15ULL * hash) % bitCount;
        bloom[bit / 8u] |= static_cast<uint8_t>(1u << (bit % 8u));
    }
}

[[nodiscard]] static bool bloomMayContain(FILE* file, const AkvlogIndexFileHeader& header, uint64_t fingerprint) {
    if (header.bloomBitCount == 0) { return false; }
    for (uint32_t hash = 0; hash < header.bloomHashCount; ++hash) {
        const uint64_t bit = mixIndexHash(fingerprint + 0x9e3779b97f4a7c15ULL * hash) % header.bloomBitCount;
        seekFile(file, INDEX_FILE_HDR_SIZE + bit / 8u);
        uint8_t byte = 0;
        if (fread(&byte, sizeof(byte), 1, file) != 1) { throw std::runtime_error("VersionLog: truncated segment Bloom filter"); }
        if ((byte & static_cast<uint8_t>(1u << (bit % 8u))) == 0) { return false; }
    }
    return true;
}

static void writeAll(FILE* file, const void* data, size_t bytes) {
    if (bytes != 0 && fwrite(data, 1, bytes, file) != bytes) { throw std::runtime_error("VersionLog: failed to write segment index"); }
}

void writeSegmentIndex(const fs::path& segmentPath, uint64_t logBytes, const SegmentKeyIndex& index) const {
    struct PreparedKey {
        uint64_t fingerprint = 0;
        const std::string* key = nullptr;
        const std::vector<IndexVersion>* versions = nullptr;
    };

    std::vector<PreparedKey> keys;
    keys.reserve(index.size());
    uint64_t versionCount = 0;
    for (const auto& [key, versions] : index) {
        if (versions.empty()) { continue; }
        if (versions.size() > std::numeric_limits<uint32_t>::max() || versionCount > std::numeric_limits<uint64_t>::max() - versions.
            size()) { throw std::runtime_error("VersionLog: segment index is too large"); }
        const uint64_t fingerprint = key.empty() ? 0ULL : core::computeKeyFp64(reinterpret_cast<const uint8_t*>(key.data()), key.size());
        keys.push_back(PreparedKey{fingerprint, &key, &versions});
        versionCount += static_cast<uint64_t>(versions.size());
    }
    std::sort(
        keys.begin(),
        keys.end(),
        [](const PreparedKey& left, const PreparedKey& right) {
            return left.fingerprint == right.fingerprint ? *left.key < *right.key : left.fingerprint < right.fingerprint;
        }
    );

    const uint64_t requestedBloomBits = std::max<uint64_t>(64, static_cast<uint64_t>(keys.size()) * INDEX_BLOOM_BITS_PER_KEY);
    if (requestedBloomBits > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error("VersionLog: segment Bloom filter is too large");
    }
    const uint32_t bloomBitCount = static_cast<uint32_t>(requestedBloomBits);
    std::vector<uint8_t> bloom((static_cast<size_t>(bloomBitCount) + 7u) / 8u, 0);
    for (const auto& key : keys) { addBloomFingerprint(bloom, bloomBitCount, key.fingerprint); }

    std::vector<AkvlogIndexKeyRecord> directories;
    std::vector<AkvlogIndexVersionRecord> versions;
    directories.reserve(keys.size());
    versions.reserve(static_cast<size_t>(versionCount));
    for (const auto& key : keys) {
        auto ordered = *key.versions;
        std::sort(
            ordered.begin(),
            ordered.end(),
            [](const IndexVersion& left, const IndexVersion& right) {
                return left.seq == right.seq ? left.offset < right.offset : left.seq < right.seq;
            }
        );
        directories.push_back(
            AkvlogIndexKeyRecord{key.fingerprint, static_cast<uint64_t>(versions.size()), static_cast<uint32_t>(ordered.size()), 0,}
        );
        for (const auto& version : ordered) { versions.push_back(AkvlogIndexVersionRecord{version.seq, version.offset}); }
    }

    const size_t payloadBytes = bloom.size() + directories.size() * INDEX_KEY_RECORD_SIZE + versions.size() * INDEX_VERSION_RECORD_SIZE;
    std::vector<uint8_t> payload;
    payload.reserve(payloadBytes);
    payload.insert(payload.end(), bloom.begin(), bloom.end());
    for (const auto& directory : directories) {
        auto encodedDirectory = encodeIndexKeyRecord(directory);
        payload.insert(payload.end(), encodedDirectory.begin(), encodedDirectory.end());
    }
    for (const auto& version : versions) {
        auto encodedVersion = encodeIndexVersionRecord(version);
        payload.insert(payload.end(), encodedVersion.begin(), encodedVersion.end());
    }

    AkvlogIndexFileHeader header{};
    header.magic = AKVLOG_INDEX_MAGIC;
    header.version = AKVLOG_INDEX_VERSION;
    header.bloomHashCount = INDEX_BLOOM_HASHES;
    header.logBytes = logBytes;
    header.keyCount = static_cast<uint64_t>(directories.size());
    header.versionCount = static_cast<uint64_t>(versions.size());
    header.bloomBitCount = bloomBitCount;
    header.payloadCrc32c = cpu::CRC32C(reinterpret_cast<const std::byte*>(payload.data()), payload.size());
    header.crc32c = 0;
    auto encodedHeader = encodeIndexHeader(header);
    header.crc32c = cpu::CRC32C(reinterpret_cast<const std::byte*>(encodedHeader.data()), encodedHeader.size());
    writeU32LeAt(encodedHeader, 40u, header.crc32c);

    const fs::path path = segmentIndexPath(segmentPath);
    forgetIndexPayloadValidation(path);
    fs::path temporary = path;
    temporary += ".tmp";
    FILE* file = nullptr;
    try {
        #ifdef _WIN32
        file = _wfopen(temporary.wstring().c_str(), L"wb");
        #else
        file = fopen(temporary.string().c_str(), "wb");
        #endif
        if (!file) { throwVLogError("cannot create segment index", temporary); }
        writeAll(file, encodedHeader.data(), encodedHeader.size());
        writeAll(file, payload.data(), payload.size());
        flushChecked(file);
        fdatasyncChecked(file);
        closeChecked(file);
        file = nullptr;

        replaceFileAtomically(temporary, path);
        markIndexPayloadValidated(path, header.payloadCrc32c);
    }
    catch (...) {
        if (file) { fclose(file); }
        std::error_code ignored;
        fs::remove(temporary, ignored);
        throw;
    }
}

void tryWriteSegmentIndex(const fs::path& segmentPath, uint64_t logBytes, const SegmentKeyIndex& index) const noexcept {
    try { writeSegmentIndex(segmentPath, logBytes, index); }
    catch (...) { sidecarRebuildFailures_.fetch_add(1, std::memory_order_relaxed); }
}

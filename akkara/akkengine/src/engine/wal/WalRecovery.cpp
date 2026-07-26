/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/wal/WalRecovery.cpp
#include "akk/engine/wal/WalRecovery.hpp"

#include "akk/engine/memtable/MemTable.hpp"
#include "akk/engine/wal/WalFraming.hpp"

#include <algorithm>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace akkaradb::engine::wal {
    namespace fs = std::filesystem;

    namespace {
        struct SegmentFile {
            fs::path path;
            WalSegmentHeader header;
        };

        struct SegmentRecoveryStatus {
            bool clean = true;
            bool replayedAny = false;
            uint64_t validBytes = WalSegmentHeader::SIZE;
        };

        [[nodiscard]] bool readExact(std::ifstream& file, uint8_t* out, size_t len) {
            if (len == 0) { return true; }
            file.read(reinterpret_cast<char*>(out), static_cast<std::streamsize>(len));
            return file.good() || file.gcount() == static_cast<std::streamsize>(len);
        }

        [[nodiscard]] std::vector<SegmentFile> listSegments(const fs::path& walDir, WalRecoveryResult& result) {
            std::vector<SegmentFile> files;
            if (!fs::exists(walDir)) { return files; }
            if (!fs::is_directory(walDir)) { throw std::runtime_error("WAL recovery path is not a directory: " + walDir.string()); }

            for (const auto& entry : fs::directory_iterator(walDir)) {
                if (!entry.is_regular_file() || entry.path().extension() != ".akwal") { continue; }
                ++result.segmentsSeen;

                std::ifstream file(entry.path(), std::ios::binary);
                if (!file) { throw std::runtime_error("WAL recovery failed to open segment: " + entry.path().string()); }

                uint8_t hdrBuf[WalSegmentHeader::SIZE]{};
                if (!readExact(file, hdrBuf, WalSegmentHeader::SIZE)) {
                    ++result.corruptSegments;
                    continue;
                }

                const WalSegmentHeader hdr = WalSegmentHeader::deserialize(hdrBuf);
                if (!hdr.verifyMagic() || !hdr.verifyVersion() || !hdr.verifyChecksum()) {
                    ++result.corruptSegments;
                    continue;
                }

                files.push_back(SegmentFile{entry.path(), hdr});
            }

            std::sort(
                files.begin(),
                files.end(),
                [](const SegmentFile& a, const SegmentFile& b) {
                    if (a.header.shardId != b.header.shardId) { return a.header.shardId < b.header.shardId; }
                    return a.header.segmentId < b.header.segmentId;
                }
            );
            return files;
        }

        [[nodiscard]] SegmentRecoveryStatus recoverSegment(
            const SegmentFile& segment,
            const WalRecoveryOptions& options,
            const WalRecovery::Callback& callback,
            WalRecoveryResult& result
        ) {
            std::ifstream file(segment.path, std::ios::binary);
            if (!file) { throw std::runtime_error("WAL recovery failed to open segment: " + segment.path.string()); }
            file.seekg(WalSegmentHeader::SIZE, std::ios::beg);

            SegmentRecoveryStatus status;
            std::vector<uint8_t> key;
            std::vector<uint8_t> value;

            while (true) {
                uint8_t ehdrBuf[WalEntryHeader::SIZE]{};
                file.read(reinterpret_cast<char*>(ehdrBuf), WalEntryHeader::SIZE);
                const std::streamsize got = file.gcount();
                if (got == 0) { break; }
                if (got != static_cast<std::streamsize>(WalEntryHeader::SIZE)) {
                    ++result.corruptSegments;
                    status.clean = false;
                    break;
                }

                const WalEntryHeader ehdr = WalEntryHeader::deserialize(ehdrBuf);
                if (!ehdr.verifyLengths(options.maxEntryBytes)) {
                    ++result.corruptSegments;
                    status.clean = false;
                    break;
                }

                key.resize(ehdr.keyLen);
                value.resize(ehdr.valueLen);
                if (!readExact(file, key.data(), key.size()) || !readExact(file, value.data(), value.size())) {
                    ++result.corruptSegments;
                    status.clean = false;
                    break;
                }

                const std::span<const uint8_t> keySpan{key.data(), key.size()};
                const std::span<const uint8_t> valueSpan{value.data(), value.size()};
                if (!ehdr.verifyChecksum(keySpan, valueSpan)) {
                    ++result.corruptSegments;
                    status.clean = false;
                    break;
                }

                ++result.entriesSeen;
                result.maxSeq = std::max(result.maxSeq, ehdr.seq);
                status.validBytes += ehdr.entryLen;

                if (ehdr.seq <= options.checkpointSeq) { continue; }

                WalRecoveredEntry out;
                out.key = key;
                out.value = value;
                out.seq = ehdr.seq;
                out.keyFp64 = ehdr.keyFp64;
                out.flags = ehdr.flags;
                out.shardId = segment.header.shardId;
                out.segmentId = segment.header.segmentId;
                callback(out);
                ++result.entriesReplayed;
                status.replayedAny = true;
            }

            if (status.replayedAny) { ++result.segmentsReplayed; }
            return status;
        }

        void truncateCorruptSegmentTail(const SegmentFile& segment, const SegmentRecoveryStatus& status, WalRecoveryResult& result) {
            const uint64_t currentSize = fs::file_size(segment.path);
            if (status.validBytes < currentSize) {
                fs::resize_file(segment.path, status.validBytes);
                ++result.segmentsTruncated;
            }
        }
    } // namespace

    WalRecoveryResult WalRecovery::recover(const WalRecoveryOptions& options, const Callback& callback) {
        if (!callback) { throw std::invalid_argument("WAL recovery callback is empty"); }
        WalRecoveryResult result{};
        const std::vector<SegmentFile> files = listSegments(options.walDir, result);
        bool skipShard = false;
        uint16_t skippedShard = 0;
        for (const SegmentFile& segment : files) {
            if (skipShard && segment.header.shardId == skippedShard) {
                if (options.truncateCorruptTail && fs::remove(segment.path)) { ++result.segmentsRemoved; }
                continue;
            }
            if (skipShard && segment.header.shardId != skippedShard) { skipShard = false; }
            const SegmentRecoveryStatus status = recoverSegment(segment, options, callback, result);
            if (!status.clean) {
                if (options.truncateCorruptTail) { truncateCorruptSegmentTail(segment, status, result); }
                skipShard = true;
                skippedShard = segment.header.shardId;
            }
        }
        return result;
    }

    WalRecoveryResult WalRecovery::recoverInto(const WalRecoveryOptions& options, memtable::MemTable& memtable) {
        WalRecoveryResult result = recover(
            options,
            [&](const WalRecoveredEntry& entry) {
                memtable.put(
                    std::span<const uint8_t>{entry.key.data(), entry.key.size()},
                    std::span<const uint8_t>{entry.value.data(), entry.value.size()},
                    entry.seq,
                    static_cast<uint8_t>(entry.flags & 0xffu),
                    entry.keyFp64,
                    0
                );
            }
        );
        if (result.maxSeq > 0) { memtable.advanceSeq(result.maxSeq); }
        return result;
    }
} // namespace akkaradb::engine::wal

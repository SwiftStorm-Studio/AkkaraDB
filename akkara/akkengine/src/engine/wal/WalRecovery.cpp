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

#include "akk/core/record/MemHdr16.hpp"
#include "akk/engine/memtable/MemTable.hpp"
#include "akk/engine/wal/WalFraming.hpp"

#include <algorithm>
#include <fstream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace akkaradb::engine::wal {
    namespace fs = std::filesystem;

    namespace {
        static constexpr std::string_view SNAPSHOT_COMMIT_MAGIC = "AKSC1";

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

        [[nodiscard]] std::optional<uint64_t> decodeSnapshotCommitCount(std::span<const uint8_t> value) noexcept {
            if (value.size() != SNAPSHOT_COMMIT_MAGIC.size() + sizeof(uint64_t)) { return std::nullopt; }
            if (!std::equal(SNAPSHOT_COMMIT_MAGIC.begin(), SNAPSHOT_COMMIT_MAGIC.end(), value.begin())) { return std::nullopt; }
            uint64_t count = 0;
            for (size_t i = 0; i < sizeof(uint64_t); ++i) {
                count |= static_cast<uint64_t>(value[SNAPSHOT_COMMIT_MAGIC.size() + i]) << (i * 8);
            }
            return count;
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
        std::unordered_map<uint64_t, std::vector<WalRecoveredEntry>> pendingSnapshotBatches;
        std::unordered_map<uint64_t, uint64_t> committedSnapshotCounts;
        uint64_t appliedMaxSeq = 0;
        const auto replayEntry = [&memtable](const WalRecoveredEntry& entry) {
            const uint8_t flags = static_cast<uint8_t>(entry.flags & 0xffu);
            if ((flags & core::MemHdr16::FLAG_TOMBSTONE) != 0) {
                memtable.remove(
                    std::span<const uint8_t>{entry.key.data(), entry.key.size()},
                    entry.seq,
                    entry.keyFp64,
                    0
                );
                return;
            }
            memtable.put(
                std::span<const uint8_t>{entry.key.data(), entry.key.size()},
                std::span<const uint8_t>{entry.value.data(), entry.value.size()},
                entry.seq,
                flags,
                entry.keyFp64,
                0
            );
        };

        WalRecoveryResult result = recover(
            options,
            [&](const WalRecoveredEntry& entry) {
                if ((entry.flags & WAL_FLAG_SNAPSHOT_COMMIT) != 0) {
                    const auto expectedCount = decodeSnapshotCommitCount(entry.value);
                    if (!expectedCount.has_value()) {
                        throw std::runtime_error("WAL recovery found corrupt snapshot commit marker");
                    }
                    committedSnapshotCounts[entry.seq] = *expectedCount;
                    return;
                }
                if ((entry.flags & WAL_FLAG_SNAPSHOT_RECORD) != 0) {
                    pendingSnapshotBatches[entry.seq].push_back(entry);
                    return;
                }
                replayEntry(entry);
                appliedMaxSeq = std::max(appliedMaxSeq, entry.seq);
            }
        );
        for (const auto& [seq, expectedCount] : committedSnapshotCounts) {
            const auto it = pendingSnapshotBatches.find(seq);
            if (expectedCount == 0 && it == pendingSnapshotBatches.end()) { continue; }
            if (it == pendingSnapshotBatches.end() || it->second.size() != expectedCount) {
                throw std::runtime_error("WAL recovery found incomplete committed snapshot transaction");
            }
            for (const auto& snapshotEntry : it->second) {
                replayEntry(snapshotEntry);
                appliedMaxSeq = std::max(appliedMaxSeq, snapshotEntry.seq);
            }
        }
        if (appliedMaxSeq > 0) { memtable.advanceSeq(appliedMaxSeq); }
        return result;
    }
} // namespace akkaradb::engine::wal

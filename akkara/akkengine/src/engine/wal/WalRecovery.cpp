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
#include "akk/cpu/CRC32C.hpp"
#include "WalCheckpoint.hpp"

#include <algorithm>
#include <array>
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
            uint64_t firstSeq = 0, lastSeq = 0;
        };

        [[nodiscard]] bool readExact(std::ifstream& file, uint8_t* out, size_t len) {
            if (len == 0) { return true; }
            file.read(reinterpret_cast<char*>(out), static_cast<std::streamsize>(len));
            return file.good() || file.gcount() == static_cast<std::streamsize>(len);
        }

        [[nodiscard]] std::vector<SegmentFile> listSegments(const WalRecoveryOptions& options, WalRecoveryResult& result) {
            const auto& walDir = options.walDir;
            std::vector<SegmentFile> files;
            if (!fs::exists(walDir)) { return files; }
            if (!fs::is_directory(walDir)) { throw std::runtime_error("WAL recovery path is not a directory: " + walDir.string()); }

            for (const auto& entry : fs::directory_iterator(walDir)) {
                if (!entry.is_regular_file() || entry.path().extension() != ".checkpoint") { continue; }
                auto segment = entry.path(); segment.replace_extension();
                if (segment.extension() != ".akwal") { continue; }
                const auto checkpoint = detail::readCheckpoint(segment);
                if (checkpoint && !fs::exists(segment)) {
                    ++result.corruptSegments;
                    if (options.truncateCorruptTail) { detail::retireCheckpoint(segment); }
                }
            }

            for (const auto& entry : fs::directory_iterator(walDir)) {
                if (!entry.is_regular_file() || entry.path().extension() != ".akwal") { continue; }
                ++result.segmentsSeen;

                std::ifstream file(entry.path(), std::ios::binary);
                if (!file) { throw std::runtime_error("WAL recovery failed to open segment: " + entry.path().string()); }

                uint8_t hdrBuf[WalSegmentHeader::SIZE]{};
                // Inspect the common prefix before reading the larger v2 header.
                constexpr size_t prefixSize = 48;
                if (!readExact(file, hdrBuf, prefixSize)) {
                    ++result.corruptSegments;
                    continue;
                }
                const auto version = static_cast<uint16_t>(hdrBuf[40] | (uint16_t{hdrBuf[41]} << 8));
                if (version == 1) {
                    throw std::runtime_error("WAL recovery: incompatible v1 segment; recreate the pre-release database");
                }
                if (!readExact(file, hdrBuf + prefixSize, WalSegmentHeader::SIZE - prefixSize)) {
                    ++result.corruptSegments;
                    continue;
                }

                WalSegmentHeader hdr = WalSegmentHeader::deserialize(hdrBuf);
                if (!hdr.verifyMagic() || !hdr.verifyVersion() || !hdr.verifyChecksum()) {
                    ++result.corruptSegments;
                    continue;
                }

                if (const auto checkpoint = detail::readCheckpoint(entry.path())) {
                    if (checkpoint->shardId != hdr.shardId || checkpoint->segmentId != hdr.segmentId) {
                        throw std::runtime_error("WAL recovery: foreign segment checkpoint");
                    }
                    hdr.publishedBytes = std::max(hdr.publishedBytes, checkpoint->publishedBytes);
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
            bool publishedBoundary = segment.header.publishedBytes == WalSegmentHeader::SIZE;
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
                if (status.firstSeq == 0 || ehdr.seq < status.firstSeq) { status.firstSeq = ehdr.seq; }
                status.lastSeq = std::max(status.lastSeq, ehdr.seq);
                if (status.validBytes == segment.header.publishedBytes) { publishedBoundary = true; }

                std::span<const uint8_t> storedValue = valueSpan;
                if ((ehdr.flags & WAL_FLAG_REQUEST_APPLICATION) != 0) {
                    constexpr size_t prefix = 5 + WAL_REQUEST_METADATA_SIZE;
                    if ((ehdr.flags & WAL_FLAG_SNAPSHOT_MASK) != 0 || value.size() < prefix ||
                        !std::equal(value.begin(), value.begin() + 5, "AKLA1")) {
                        throw std::runtime_error("WAL recovery found invalid request application envelope");
                    }
                    if (options.requestApplication) { options.requestApplication(valueSpan.subspan(5, WAL_REQUEST_METADATA_SIZE), ehdr.seq); }
                    storedValue = valueSpan.subspan(prefix);
                }
                if (ehdr.seq <= options.checkpointSeq) { continue; }

                WalRecoveredEntry out;
                out.key = key;
                out.value.assign(storedValue.begin(), storedValue.end());
                out.seq = ehdr.seq;
                out.keyFp64 = ehdr.keyFp64;
                out.flags = static_cast<uint16_t>(ehdr.flags & ~WAL_FLAG_REQUEST_APPLICATION);
                out.shardId = segment.header.shardId;
                out.segmentId = segment.header.segmentId;
                callback(out);
                ++result.entriesReplayed;
                status.replayedAny = true;
            }

            // A record-aligned EOF is still loss if the CRC-covered header
            // published a longer prefix. Another shard's maximum seq cannot prove it.
            if (status.clean && (!publishedBoundary || segment.header.publishedBytes > status.validBytes)) {
                ++result.corruptSegments;
                status.clean = false;
            }
            if (status.replayedAny) { ++result.segmentsReplayed; }
            return status;
        }

        void truncateCorruptSegmentTail(const SegmentFile& segment, const SegmentRecoveryStatus& status, WalRecoveryResult& result) {
            const uint64_t currentSize = fs::file_size(segment.path);
            if (status.validBytes < currentSize) {
                fs::resize_file(segment.path, status.validBytes);
            }
            auto header = segment.header;
            header.firstSeq = status.firstSeq; header.lastSeq = status.lastSeq;
            header.publishedBytes = status.validBytes; header.crc32c = 0;
            std::array<uint8_t, WalSegmentHeader::SIZE> bytes{}; header.serialize(bytes.data());
            header.crc32c = cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size());
            header.serialize(bytes.data());
            std::fstream output{segment.path, std::ios::binary | std::ios::in | std::ios::out};
            output.write(reinterpret_cast<const char*>(bytes.data()), bytes.size()); output.flush();
            if (!output) { throw std::runtime_error("WAL recovery failed to publish truncated prefix: " + segment.path.string()); }
            output.close();
            detail::syncRepairedSegment(segment.path);
            detail::publishCheckpoint(segment.path, header);
            ++result.segmentsTruncated;
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
        const std::vector<SegmentFile> files = listSegments(options, result);
        bool skipShard = false;
        uint16_t skippedShard = 0;
        for (const SegmentFile& segment : files) {
            if (skipShard && segment.header.shardId == skippedShard) {
                if (options.truncateCorruptTail) {
                    detail::retireCheckpoint(segment.path);
                    if (fs::remove(segment.path)) { ++result.segmentsRemoved; }
                }
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
        std::unordered_map<uint64_t, std::unordered_map<std::string, size_t>> snapshotKeyPositions;
        std::unordered_map<uint64_t, uint64_t> committedSnapshotCounts;
        uint64_t appliedMaxSeq = 0;
        const auto replayEntry = [&memtable](const WalRecoveredEntry& entry) {
            const uint8_t flags = static_cast<uint8_t>(entry.flags & 0xffu);
            if ((flags & core::MemHdr16::FLAG_TOMBSTONE) != 0) {
                memtable.remove(std::span<const uint8_t>{entry.key.data(), entry.key.size()}, entry.seq, entry.keyFp64, 0);
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
                    if (!expectedCount.has_value()) { throw std::runtime_error("WAL recovery found corrupt snapshot commit marker"); }
                    committedSnapshotCounts[entry.seq] = *expectedCount;
                    return;
                }
                if ((entry.flags & WAL_FLAG_SNAPSHOT_RECORD) != 0) {
                    auto& batch = pendingSnapshotBatches[entry.seq];
                    auto& positions = snapshotKeyPositions[entry.seq];
                    const std::string key{reinterpret_cast<const char*>(entry.key.data()), entry.key.size()};
                    const auto [position, inserted] = positions.emplace(key, batch.size());
                    if (inserted) { batch.push_back(entry); }
                    else {
                        // Recovery may restart an interrupted install of the same
                        // fixed snapshot. WAL shards do not preserve global order,
                        // so accept identical records rather than a begin marker.
                        const auto& previous = batch[position->second];
                        if (previous.flags != entry.flags || previous.value != entry.value || previous.keyFp64 != entry.keyFp64) {
                            throw std::runtime_error("WAL recovery found conflicting snapshot records");
                        }
                    }
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

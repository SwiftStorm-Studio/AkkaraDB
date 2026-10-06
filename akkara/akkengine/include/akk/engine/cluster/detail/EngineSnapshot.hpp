/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
#pragma once
#include <cstdint>
#include <span>
#include <stdexcept>
#include <vector>

namespace akkaradb::engine::cluster::detail {
    enum class SnapshotRecordKind : uint8_t { STATE = 0, HEAD = 1, HISTORY = 2 };
    struct SnapshotRecordKey {
        SnapshotRecordKind kind = SnapshotRecordKind::HEAD;
        uint8_t flags = 0;
        uint64_t sequence = 0, source = 0, timestamp = 0;
        std::span<const uint8_t> key;
        uint64_t blobId = 0;
    };
    inline std::vector<uint8_t> encodeSnapshotKey(const SnapshotRecordKey& record) {
        std::vector<uint8_t> bytes{'A', 'K', 'E', 'S', '1', static_cast<uint8_t>(record.kind), record.flags};
        for (const auto value : {record.sequence, record.source, record.timestamp, record.blobId}) {
            for (unsigned i = 0; i < 8; ++i) { bytes.push_back(static_cast<uint8_t>(value >> (8 * i))); }
        }
        bytes.insert(bytes.end(), record.key.begin(), record.key.end());
        return bytes;
    }
    inline SnapshotRecordKey decodeSnapshotKey(std::span<const uint8_t> bytes) {
        if (bytes.size() < 39 || bytes[0] != 'A' || bytes[1] != 'K' || bytes[2] != 'E' || bytes[3] != 'S' ||
            bytes[4] != '1' || bytes[5] > 2 || (bytes[6] & ~uint8_t{0x0f}) != 0) {
            throw std::runtime_error("Cluster snapshot: invalid state record key");
        }
        const auto integer = [&](size_t offset) {
            uint64_t value = 0;
            for (unsigned i = 0; i < 8; ++i) { value |= uint64_t{bytes[offset + i]} << (8 * i); }
            return value;
        };
        if (((bytes[6] & 2) != 0) != (integer(31) != 0)) { throw std::runtime_error("Cluster snapshot: invalid Blob identity"); }
        return {static_cast<SnapshotRecordKind>(bytes[5]), bytes[6], integer(7), integer(15), integer(23), bytes.subspan(39), integer(31)};
    }
}

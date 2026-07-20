/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#pragma once

#include "akk/engine/AkkEngine.hpp"
#include "akk/engine/server/ApiFraming.hpp"
#include "akkaradb/Stats.hpp"

#include <cstdint>
#include <span>
#include <vector>

namespace akkaradb::engine::server::tcp {
    [[nodiscard]] bool readBatchCount(std::span<const uint8_t> payload, uint32_t& out) noexcept;
    [[nodiscard]] bool readU64(std::span<const uint8_t> payload, uint64_t& out) noexcept;
    [[nodiscard]] bool readScanPayload(std::span<const uint8_t> payload, uint32_t& limit, std::span<const uint8_t>& endKey) noexcept;
    [[nodiscard]] bool readOptionalLimit(std::span<const uint8_t> payload, uint32_t& limit) noexcept;

    void encodeBoolPayload(bool value, std::vector<uint8_t>& out);
    void encodeU64Payload(uint64_t value, std::vector<uint8_t>& out);
    void encodeScanPayload(std::span<const AkkEngine::ScanRecordView> records, bool truncated, std::vector<uint8_t>& out);
    void encodeHistoryPayload(std::span<const VersionEntry> entries, std::vector<uint8_t>& out);
    void encodeScanStreamPayload(const AkkEngine::ScanRecordView& record, std::vector<uint8_t>& out);
    void encodeHistoryStreamPayload(const VersionEntry& entry, std::vector<uint8_t>& out);
    void encodeStreamEndPayload(uint32_t emitted, bool truncated, std::vector<uint8_t>& out);
    void encodeStatsPayload(const EngineStats& stats, std::vector<uint8_t>& out);
}

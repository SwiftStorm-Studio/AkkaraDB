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

// akkengine/include/akk/engine/server/ApiFraming.hpp
#pragma once

#include <cstdint>
#include <span>
#include <vector>

#include "akk/engine/server/AkkApiServerExport.hpp"

namespace akkaradb::engine::server {
    enum class ApiOp : uint8_t {
        GET = 0x01,
        PUT = 0x02,
        REMOVE = 0x03,
        GET_AT = 0x04,
        BATCH_PUT = 0x05,
        BATCH_GET = 0x06,
        PING = 0x07,
        EXISTS = 0x08,
        COUNT = 0x09,
        SCAN = 0x0A,
        HISTORY = 0x0B,
        ROLLBACK_TO = 0x0C,
        ROLLBACK_KEY = 0x0D,
        FORCE_SYNC = 0x0E,
        FORCE_FLUSH = 0x0F,
        STATS = 0x10, };

    enum class ApiStatus : uint8_t {
        OK = 0x00, NOT_FOUND = 0x01, ERROR_STATUS = 0xFF,
    };

    #pragma pack(push, 1)
    struct ApiRequestHeader {
        char magic[4];
        uint8_t version;
        ApiOp opcode;
        uint32_t requestId;
        uint16_t keyLen;
        uint32_t valLen;
    };

    struct ApiResponseHeader {
        char magic[4];
        ApiStatus status;
        uint32_t requestId;
        uint32_t valLen;
    };
    #pragma pack(pop)

    static_assert(sizeof(ApiRequestHeader) == 16);
    static_assert(sizeof(ApiResponseHeader) == 13);

    inline constexpr char REQUEST_MAGIC[4] = {'A', 'K', '5', 'Q'};
    inline constexpr char RESPONSE_MAGIC[4] = {'A', 'K', '5', 'S'};
    inline constexpr uint8_t PROTOCOL_VERSION = 2;

    struct ApiBatchPutItem {
        std::span<const uint8_t> key;
        std::span<const uint8_t> value;
    };

    struct ApiBatchGetResult {
        ApiStatus status = ApiStatus::ERROR_STATUS;
        std::span<const uint8_t> value;
    };

    [[nodiscard]] AKKARADB_API_SERVER_API uint32_t crc32c(std::span<const uint8_t> data) noexcept;
    [[nodiscard]] AKKARADB_API_SERVER_API uint32_t crc32c(std::span<const uint8_t> first, std::span<const uint8_t> second);

    [[nodiscard]] AKKARADB_API_SERVER_API bool decodeBatchPut(
        std::span<const uint8_t> payload,
        uint32_t maxItems,
        std::vector<ApiBatchPutItem>& out
    );
    [[nodiscard]] AKKARADB_API_SERVER_API bool decodeBatchGet(
        std::span<const uint8_t> payload,
        uint32_t maxItems,
        std::vector<std::span<const uint8_t>>& out
    );
    AKKARADB_API_SERVER_API void encodeResponse(
        ApiStatus status,
        uint32_t requestId,
        std::span<const uint8_t> value,
        std::vector<uint8_t>& out
    );
    AKKARADB_API_SERVER_API void encodeBatchGetResponse(
        uint32_t requestId,
        std::span<const ApiBatchGetResult> results,
        std::vector<uint8_t>& out
    );
    AKKARADB_API_SERVER_API void encodeError(uint32_t requestId, std::vector<uint8_t>& out);
}

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

namespace akkaradb::engine::server {
    enum class ApiOp : uint8_t {
        Get = 0x01, Put = 0x02, Remove = 0x03, GetAt = 0x04, BatchPut = 0x05, BatchGet = 0x06,
    };

    enum class ApiStatus : uint8_t {
        Ok = 0x00, NotFound = 0x01, Error = 0xFF,
    };

    #pragma pack(push, 1)
    struct ApiRequestHeader {
        char magic[4];
        uint8_t version;
        ApiOp opcode;
        uint32_t request_id;
        uint16_t key_len;
        uint32_t val_len;
    };

    struct ApiResponseHeader {
        char magic[4];
        ApiStatus status;
        uint32_t request_id;
        uint32_t val_len;
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
        ApiStatus status = ApiStatus::Error;
        std::span<const uint8_t> value;
    };

    [[nodiscard]] uint32_t crc32c(std::span<const uint8_t> data) noexcept;
    [[nodiscard]] uint32_t crc32c(std::span<const uint8_t> first, std::span<const uint8_t> second);

    [[nodiscard]] bool decode_batch_put(std::span<const uint8_t> payload, uint32_t max_items, std::vector<ApiBatchPutItem>& out);
    [[nodiscard]] bool decode_batch_get(std::span<const uint8_t> payload, uint32_t max_items, std::vector<std::span<const uint8_t>>& out);
    void encode_response(ApiStatus status, uint32_t request_id, std::span<const uint8_t> value, std::vector<uint8_t>& out);
    void encode_batch_get_response(uint32_t request_id, std::span<const ApiBatchGetResult> results, std::vector<uint8_t>& out);
    void encode_error(uint32_t request_id, std::vector<uint8_t>& out);
}

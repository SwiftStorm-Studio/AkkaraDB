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

// akkengine/src/engine/server/ApiFraming.cpp
#include "akk/engine/server/ApiFraming.hpp"

#include "akk/cpu/CRC32C.hpp"

#include <cstddef>
#include <cstring>

namespace akkaradb::engine::server {
    uint32_t crc32c(std::span<const uint8_t> data) noexcept {
        return cpu::CRC32C(reinterpret_cast<const std::byte*>(data.data()), data.size());
    }

    uint32_t crc32c(std::span<const uint8_t> first, std::span<const uint8_t> second) {
        if (first.empty()) { return crc32c(second); }
        if (second.empty()) { return crc32c(first); }

        std::vector<uint8_t> combined;
        combined.reserve(first.size() + second.size());
        combined.insert(combined.end(), first.begin(), first.end());
        combined.insert(combined.end(), second.begin(), second.end());
        return crc32c(std::span<const uint8_t>{combined.data(), combined.size()});
    }

    void encode_response(ApiStatus status, uint32_t request_id, std::span<const uint8_t> value, std::vector<uint8_t>& out) {
        ApiResponseHeader header{};
        std::memcpy(header.magic, RESPONSE_MAGIC, sizeof(header.magic));
        header.status = status;
        header.request_id = request_id;
        header.val_len = static_cast<uint32_t>(value.size());

        const uint32_t checksum = crc32c(value);
        out.resize(sizeof(header) + value.size() + sizeof(checksum));

        uint8_t* p = out.data();
        std::memcpy(p, &header, sizeof(header));
        p += sizeof(header);
        if (!value.empty()) {
            std::memcpy(p, value.data(), value.size());
            p += value.size();
        }
        std::memcpy(p, &checksum, sizeof(checksum));
    }

    void encode_error(uint32_t request_id, std::vector<uint8_t>& out) { encode_response(ApiStatus::Error, request_id, {}, out); }
}

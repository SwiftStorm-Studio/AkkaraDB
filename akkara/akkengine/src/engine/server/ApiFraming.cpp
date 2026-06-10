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
#include <limits>

namespace akkaradb::engine::server {
    namespace {
        constexpr uint32_t kCrc32cPolynomial = 0x82F63B78u;

        [[nodiscard]] uint32_t gf2_matrix_times(const uint32_t* mat, uint32_t vec) noexcept {
            uint32_t sum = 0;
            size_t index = 0;
            while (vec != 0) {
                if ((vec & 1u) != 0) { sum ^= mat[index]; }
                vec >>= 1;
                ++index;
            }
            return sum;
        }

        void gf2_matrix_square(uint32_t* square, const uint32_t* mat) noexcept {
            for (size_t i = 0; i < 32; ++i) { square[i] = gf2_matrix_times(mat, mat[i]); }
        }

        [[nodiscard]] uint32_t crc32c_combine(uint32_t first_crc, uint32_t second_crc, size_t second_len) noexcept {
            if (second_len == 0) { return first_crc; }

            uint32_t odd[32]{};
            uint32_t even[32]{};

            odd[0] = kCrc32cPolynomial;
            uint32_t row = 1;
            for (size_t i = 1; i < 32; ++i) {
                odd[i] = row;
                row <<= 1;
            }

            gf2_matrix_square(even, odd);
            gf2_matrix_square(odd, even);

            do {
                gf2_matrix_square(even, odd);
                if ((second_len & 1u) != 0) { first_crc = gf2_matrix_times(even, first_crc); }
                second_len >>= 1;
                if (second_len == 0) { break; }

                gf2_matrix_square(odd, even);
                if ((second_len & 1u) != 0) { first_crc = gf2_matrix_times(odd, first_crc); }
                second_len >>= 1;
            }
            while (second_len != 0);

            return first_crc ^ second_crc;
        }

        template <typename T>
        [[nodiscard]] bool read_plain(std::span<const uint8_t> payload, size_t& pos, T& out) noexcept {
            if (pos > payload.size() || payload.size() - pos < sizeof(T)) { return false; }
            std::memcpy(&out, payload.data() + pos, sizeof(T));
            pos += sizeof(T);
            return true;
        }

        template <typename T>
        void write_plain(uint8_t*& out, T value) noexcept {
            std::memcpy(out, &value, sizeof(T));
            out += sizeof(T);
        }
    }

    uint32_t crc32c(std::span<const uint8_t> data) noexcept {
        return cpu::CRC32C(reinterpret_cast<const std::byte*>(data.data()), data.size());
    }

    uint32_t crc32c(std::span<const uint8_t> first, std::span<const uint8_t> second) {
        if (first.empty()) { return crc32c(second); }
        if (second.empty()) { return crc32c(first); }

        return crc32c_combine(crc32c(first), crc32c(second), second.size());
    }

    bool decode_batch_put(std::span<const uint8_t> payload, uint32_t max_items, std::vector<ApiBatchPutItem>& out) {
        out.clear();

        size_t pos = 0;
        uint32_t count = 0;
        if (!read_plain(payload, pos, count)) { return false; }
        if (count > max_items) { return false; }
        out.reserve(count);

        for (uint32_t i = 0; i < count; ++i) {
            uint16_t key_len = 0;
            uint32_t value_len = 0;
            if (!read_plain(payload, pos, key_len) || !read_plain(payload, pos, value_len)) { return false; }
            if (payload.size() - pos < static_cast<size_t>(key_len) + static_cast<size_t>(value_len)) { return false; }

            const uint8_t* key = payload.data() + pos;
            pos += key_len;
            const uint8_t* value = payload.data() + pos;
            pos += value_len;
            out.push_back(ApiBatchPutItem{{key, key_len}, {value, value_len}});
        }

        return pos == payload.size();
    }

    bool decode_batch_get(std::span<const uint8_t> payload, uint32_t max_items, std::vector<std::span<const uint8_t>>& out) {
        out.clear();

        size_t pos = 0;
        uint32_t count = 0;
        if (!read_plain(payload, pos, count)) { return false; }
        if (count > max_items) { return false; }
        out.reserve(count);

        for (uint32_t i = 0; i < count; ++i) {
            uint16_t key_len = 0;
            if (!read_plain(payload, pos, key_len)) { return false; }
            if (payload.size() - pos < key_len) { return false; }

            const uint8_t* key = payload.data() + pos;
            pos += key_len;
            out.push_back({key, key_len});
        }

        return pos == payload.size();
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

    void encode_batch_get_response(uint32_t request_id, std::span<const ApiBatchGetResult> results, std::vector<uint8_t>& out) {
        size_t payload_size = sizeof(uint32_t);
        for (const auto& result : results) {
            if (result.value.size() > std::numeric_limits<uint32_t>::max()) {
                encode_error(request_id, out);
                return;
            }
            payload_size += sizeof(uint8_t) + sizeof(uint32_t) + result.value.size();
        }

        ApiResponseHeader header{};
        std::memcpy(header.magic, RESPONSE_MAGIC, sizeof(header.magic));
        header.status = ApiStatus::Ok;
        header.request_id = request_id;
        if (payload_size > std::numeric_limits<uint32_t>::max()) {
            encode_error(request_id, out);
            return;
        }
        header.val_len = static_cast<uint32_t>(payload_size);

        out.resize(sizeof(header) + payload_size + sizeof(uint32_t));

        uint8_t* p = out.data();
        std::memcpy(p, &header, sizeof(header));
        p += sizeof(header);

        uint8_t* payload = p;
        write_plain(p, static_cast<uint32_t>(results.size()));
        for (const auto& result : results) {
            write_plain(p, static_cast<uint8_t>(result.status));
            write_plain(p, static_cast<uint32_t>(result.value.size()));
            if (!result.value.empty()) {
                std::memcpy(p, result.value.data(), result.value.size());
                p += result.value.size();
            }
        }

        const uint32_t checksum = crc32c(std::span<const uint8_t>{payload, payload_size});
        write_plain(p, checksum);
    }

    void encode_error(uint32_t request_id, std::vector<uint8_t>& out) { encode_response(ApiStatus::Error, request_id, {}, out); }
}

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

// akkserver/src/tcp/ApiFraming.cpp
#include "akk/engine/server/ApiFraming.hpp"

#include "akk/cpu/CRC32C.hpp"

#include <cstddef>
#include <cstring>
#include <limits>

namespace akkaradb::engine::server {
    namespace {
        constexpr uint32_t kCrc32cPolynomial = 0x82F63B78u;

        [[nodiscard]] uint32_t gf2MatrixTimes(const uint32_t* mat, uint32_t vec) noexcept {
            uint32_t sum = 0;
            size_t index = 0;
            while (vec != 0) {
                if ((vec & 1u) != 0) { sum ^= mat[index]; }
                vec >>= 1;
                ++index;
            }
            return sum;
        }

        void gf2MatrixSquare(uint32_t* square, const uint32_t* mat) noexcept {
            for (size_t i = 0; i < 32; ++i) { square[i] = gf2MatrixTimes(mat, mat[i]); }
        }

        [[nodiscard]] uint32_t crc32cCombine(uint32_t firstCrc, uint32_t secondCrc, size_t secondLen) noexcept {
            if (secondLen == 0) { return firstCrc; }

            uint32_t odd[32]{};
            uint32_t even[32]{};

            odd[0] = kCrc32cPolynomial;
            uint32_t row = 1;
            for (size_t i = 1; i < 32; ++i) {
                odd[i] = row;
                row <<= 1;
            }

            gf2MatrixSquare(even, odd);
            gf2MatrixSquare(odd, even);

            do {
                gf2MatrixSquare(even, odd);
                if ((secondLen & 1u) != 0) { firstCrc = gf2MatrixTimes(even, firstCrc); }
                secondLen >>= 1;
                if (secondLen == 0) { break; }

                gf2MatrixSquare(odd, even);
                if ((secondLen & 1u) != 0) { firstCrc = gf2MatrixTimes(odd, firstCrc); }
                secondLen >>= 1;
            }
            while (secondLen != 0);

            return firstCrc ^ secondCrc;
        }

        template <typename T>
        [[nodiscard]] bool readPlain(std::span<const uint8_t> payload, size_t& pos, T& out) noexcept {
            if (pos > payload.size() || payload.size() - pos < sizeof(T)) { return false; }
            std::memcpy(&out, payload.data() + pos, sizeof(T));
            pos += sizeof(T);
            return true;
        }

        template <typename T>
        void writePlain(uint8_t*& out, T value) noexcept {
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

        return crc32cCombine(crc32c(first), crc32c(second), second.size());
    }

    bool decodeBatchPut(std::span<const uint8_t> payload, uint32_t maxItems, std::vector<ApiBatchPutItem>& out) {
        out.clear();

        size_t pos = 0;
        uint32_t count = 0;
        if (!readPlain(payload, pos, count)) { return false; }
        if (count > maxItems) { return false; }
        out.reserve(count);

        for (uint32_t i = 0; i < count; ++i) {
            uint16_t keyLen = 0;
            uint32_t valueLen = 0;
            if (!readPlain(payload, pos, keyLen) || !readPlain(payload, pos, valueLen)) { return false; }
            if (payload.size() - pos < static_cast<size_t>(keyLen) + static_cast<size_t>(valueLen)) { return false; }

            const uint8_t* key = payload.data() + pos;
            pos += keyLen;
            const uint8_t* value = payload.data() + pos;
            pos += valueLen;
            out.push_back(ApiBatchPutItem{{key, keyLen}, {value, valueLen}});
        }

        return pos == payload.size();
    }

    bool decodeBatchGet(std::span<const uint8_t> payload, uint32_t maxItems, std::vector<std::span<const uint8_t>>& out) {
        out.clear();

        size_t pos = 0;
        uint32_t count = 0;
        if (!readPlain(payload, pos, count)) { return false; }
        if (count > maxItems) { return false; }
        out.reserve(count);

        for (uint32_t i = 0; i < count; ++i) {
            uint16_t keyLen = 0;
            if (!readPlain(payload, pos, keyLen)) { return false; }
            if (payload.size() - pos < keyLen) { return false; }

            const uint8_t* key = payload.data() + pos;
            pos += keyLen;
            out.push_back({key, keyLen});
        }

        return pos == payload.size();
    }

    void encodeResponse(ApiStatus status, uint32_t requestId, std::span<const uint8_t> value, std::vector<uint8_t>& out) {
        ApiResponseHeader header{};
        std::memcpy(header.magic, RESPONSE_MAGIC, sizeof(header.magic));
        header.status = status;
        header.requestId = requestId;
        header.valLen = static_cast<uint32_t>(value.size());

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

    void encodeBatchGetResponse(uint32_t requestId, std::span<const ApiBatchGetResult> results, std::vector<uint8_t>& out) {
        size_t payloadSize = sizeof(uint32_t);
        for (const auto& result : results) {
            if (result.value.size() > std::numeric_limits<uint32_t>::max()) {
                encodeError(requestId, out);
                return;
            }
            payloadSize += sizeof(uint8_t) + sizeof(uint32_t) + result.value.size();
        }

        ApiResponseHeader header{};
        std::memcpy(header.magic, RESPONSE_MAGIC, sizeof(header.magic));
        header.status = ApiStatus::OK;
        header.requestId = requestId;
        if (payloadSize > std::numeric_limits<uint32_t>::max()) {
            encodeError(requestId, out);
            return;
        }
        header.valLen = static_cast<uint32_t>(payloadSize);

        out.resize(sizeof(header) + payloadSize + sizeof(uint32_t));

        uint8_t* p = out.data();
        std::memcpy(p, &header, sizeof(header));
        p += sizeof(header);

        uint8_t* payload = p;
        writePlain(p, static_cast<uint32_t>(results.size()));
        for (const auto& result : results) {
            writePlain(p, static_cast<uint8_t>(result.status));
            writePlain(p, static_cast<uint32_t>(result.value.size()));
            if (!result.value.empty()) {
                std::memcpy(p, result.value.data(), result.value.size());
                p += result.value.size();
            }
        }

        const uint32_t checksum = crc32c(std::span<const uint8_t>{payload, payloadSize});
        writePlain(p, checksum);
    }

    void encodeError(uint32_t requestId, std::vector<uint8_t>& out) { encodeResponse(ApiStatus::ERROR_STATUS, requestId, {}, out); }
}

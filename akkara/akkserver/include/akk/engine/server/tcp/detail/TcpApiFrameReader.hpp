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

#include "akk/engine/server/ApiFraming.hpp"
#include "akk/engine/server/ApiTransport.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace akkaradb::engine::server::tcp {
    struct RequestFrame {
        ApiRequestHeader header{};
        std::span<const uint8_t> key;
        std::span<const uint8_t> value;
        uint32_t receivedCrc = 0;
        size_t wireSize = 0;
        bool requestIdUsable = false;
    };

    enum class FrameReadStatus : uint8_t {
        OK, NEED_MORE, CLOSED, INVALID,
    };

    class BufferedInput {
        public:
            BufferedInput();

            FrameReadStatus readFrame(detail::Connection& connection, RequestFrame& out, bool blocking);
            void consume(size_t size) noexcept;

        private:
            [[nodiscard]] size_t available() const noexcept;
            bool ensure(detail::Connection& connection, size_t size, bool blocking);
            void compact();
            bool refill(detail::Connection& connection);

            std::vector<uint8_t> buffer_;
            size_t pos_ = 0;
    };
}

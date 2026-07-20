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

#include "akk/engine/server/tcp/detail/TcpApiFrameReader.hpp"

#include <cstring>
#include <limits>

namespace akkaradb::engine::server::tcp {
    namespace {
        constexpr uint32_t kMaxValueBytes = 64u * 1024u * 1024u;
        constexpr size_t kReadBufferBytes = 64u * 1024u;

        [[nodiscard]] bool validMagic(const ApiRequestHeader& header) noexcept {
            return std::memcmp(header.magic, REQUEST_MAGIC, sizeof(header.magic)) == 0;
        }

        [[nodiscard]] bool validHeader(const ApiRequestHeader& header) noexcept {
            return validMagic(header) && header.version == PROTOCOL_VERSION && header.valLen <= kMaxValueBytes;
        }
    }

    BufferedInput::BufferedInput() { buffer_.reserve(kReadBufferBytes); }

    FrameReadStatus BufferedInput::readFrame(detail::Connection& connection, RequestFrame& out, bool blocking) {
        if (!ensure(connection, sizeof(ApiRequestHeader), blocking)) {
            return blocking ? FrameReadStatus::CLOSED : FrameReadStatus::NEED_MORE;
        }

        std::memcpy(&out.header, buffer_.data() + pos_, sizeof(out.header));
        out.requestIdUsable = validMagic(out.header);
        out.wireSize = sizeof(ApiRequestHeader);
        if (!validHeader(out.header)) { return FrameReadStatus::INVALID; }

        const size_t keyLen = out.header.keyLen;
        const size_t valueLen = out.header.valLen;
        if (valueLen > kMaxValueBytes) { return FrameReadStatus::INVALID; }
        if (keyLen > std::numeric_limits<size_t>::max() - sizeof(ApiRequestHeader) - sizeof(uint32_t) - valueLen) {
            return FrameReadStatus::INVALID;
        }

        const size_t total = sizeof(ApiRequestHeader) + keyLen + valueLen + sizeof(uint32_t);
        if (!ensure(connection, total, blocking)) { return blocking ? FrameReadStatus::CLOSED : FrameReadStatus::NEED_MORE; }

        const uint8_t* p = buffer_.data() + pos_ + sizeof(ApiRequestHeader);
        out.key = {p, keyLen};
        p += keyLen;
        out.value = {p, valueLen};
        p += valueLen;
        std::memcpy(&out.receivedCrc, p, sizeof(out.receivedCrc));
        out.wireSize = total;
        return FrameReadStatus::OK;
    }

    void BufferedInput::consume(size_t size) noexcept {
        pos_ += size;
        if (pos_ >= buffer_.size()) {
            buffer_.clear();
            pos_ = 0;
        }
    }

    size_t BufferedInput::available() const noexcept { return buffer_.size() - pos_; }

    bool BufferedInput::ensure(detail::Connection& connection, size_t size, bool blocking) {
        while (available() < size) {
            if (!blocking) { return false; }
            if (!refill(connection)) { return false; }
        }
        return true;
    }

    void BufferedInput::compact() {
        if (pos_ == 0) { return; }
        if (pos_ >= buffer_.size()) {
            buffer_.clear();
            pos_ = 0;
            return;
        }

        std::memmove(buffer_.data(), buffer_.data() + pos_, buffer_.size() - pos_);
        buffer_.resize(buffer_.size() - pos_);
        pos_ = 0;
    }

    bool BufferedInput::refill(detail::Connection& connection) {
        compact();
        const size_t offset = buffer_.size();
        buffer_.resize(offset + kReadBufferBytes);
        const size_t got = connection.recvSome(buffer_.data() + offset, kReadBufferBytes);
        if (got == 0) {
            buffer_.resize(offset);
            return false;
        }
        buffer_.resize(offset + got);
        return true;
    }
}

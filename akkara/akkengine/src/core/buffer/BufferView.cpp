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

// akkengine/src/core/buffer/BufferView.cpp
#include "akk/core/buffer/BufferView.hpp"
#include "akk/core/buffer/OwnedBuffer.hpp"
#include "akk/cpu/CRC32C.hpp"

#include <bit>
#include <cstring>
#include <stdexcept>

static_assert(std::endian::native == std::endian::little, "AkkaraDB requires Little-Endian architecture");

namespace akkaradb::core {
    // ==================== Slice ====================

    BufferView BufferView::slice(size_t offset, size_t length) const {
        checkBounds(offset, length);
        const std::byte* start = (size_ == 0) ? data_ : data_ + offset;
        return BufferView{start, length};
    }

    BufferView BufferView::slice(size_t offset) const {
        checkBounds(offset, 0);
        const std::byte* start = (size_ == 0) ? data_ : data_ + offset;
        return BufferView{start, size_ - offset};
    }

    // ==================== Ownership ====================

    OwnedBuffer BufferView::toOwned() const {
        if (size_ == 0) { return OwnedBuffer::allocate(0); }

        auto out = OwnedBuffer::allocate(size_);
        std::memcpy(out.data(), data_, size_);
        return out;
    }

    // ==================== Little-Endian Reads ====================

    uint8_t BufferView::readU8(size_t offset) const {
        checkBounds(offset, 1);
        return static_cast<uint8_t>(data_[offset]);
    }

    uint16_t BufferView::readU16Le(size_t offset) const {
        checkBounds(offset, 2);
        uint16_t v;
        std::memcpy(&v, data_ + offset, sizeof(v));
        return v;
    }

    uint32_t BufferView::readU32Le(size_t offset) const {
        checkBounds(offset, 4);
        uint32_t v;
        std::memcpy(&v, data_ + offset, sizeof(v));
        return v;
    }

    uint64_t BufferView::readU64Le(size_t offset) const {
        checkBounds(offset, 8);
        uint64_t v;
        std::memcpy(&v, data_ + offset, sizeof(v));
        return v;
    }

    // ==================== CRC32C ====================

    uint32_t BufferView::crc32c(size_t offset, size_t length) const {
        checkBounds(offset, length);
        const std::byte* start = (length == 0) ? data_ : data_ + offset;
        return cpu::CRC32C(start, length);
    }

    // ==================== String ====================

    std::string_view BufferView::asStringView(size_t offset, size_t length) const {
        checkBounds(offset, length);
        const std::byte* start = (length == 0) ? data_ : data_ + offset;
        return {reinterpret_cast<const char*>(start), length};
    }

    // ==================== Bounds ====================

    void BufferView::checkBounds(size_t offset, size_t length) const {
        if (offset > size_ || length > size_ - offset) { throw std::out_of_range("BufferView: out of range"); }
    }
} // namespace akkaradb::core

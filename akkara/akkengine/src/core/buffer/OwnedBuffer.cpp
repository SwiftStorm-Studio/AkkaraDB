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

// akkengine/src/core/buffer/OwnedBuffer.cpp
#include "akk/core/buffer/OwnedBuffer.hpp"
#include "akk/core/buffer/BufferView.hpp"

#include <new>

namespace akkaradb::core {
    namespace {
        // ==================== Heap Deleter ====================

        void heap_deleter(void* ptr, size_t /*size*/, void* /*ctx*/) { operator delete(ptr); }
    }

    // ==================== Factory ====================

    OwnedBuffer OwnedBuffer::allocate(size_t size) {
        if (size == 0) { return OwnedBuffer{}; }

        void* ptr = operator new(size);

        return {static_cast<std::byte*>(ptr), size, &heap_deleter, nullptr};
    }

    // ==================== View ====================

    BufferView OwnedBuffer::as_view() const noexcept { return BufferView{data_, size_}; }
} // namespace akkaradb::core

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/core/buffer/OwnedBuffer.cpp
#include "akk/core/buffer/OwnedBuffer.hpp"
#include "akk/core/buffer/BufferView.hpp"

#include <new>

namespace akkaradb::core {
    namespace {
        // ==================== Heap Deleter ====================

        void heapDeleter(void* ptr, size_t /*size*/, void* /*ctx*/) { operator delete(ptr); }
    }

    // ==================== Factory ====================

    OwnedBuffer OwnedBuffer::allocate(size_t size) {
        if (size == 0) { return OwnedBuffer{}; }

        void* ptr = operator new(size);

        return {static_cast<std::byte*>(ptr), size, &heapDeleter, nullptr};
    }

    // ==================== View ====================

    BufferView OwnedBuffer::asView() const noexcept { return BufferView{data_, size_}; }
} // namespace akkaradb::core

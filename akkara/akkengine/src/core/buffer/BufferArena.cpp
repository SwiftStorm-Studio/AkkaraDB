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

// akkengine/src/core/buffer/BufferArena.cpp
#include "akk/core/buffer/BufferArena.hpp"

#include <algorithm>
#include <new>
#include <stdexcept>

namespace akkaradb::core {
    BufferArena::BufferArena(size_t initialBlockSize, size_t maxBlockSize)
        : initialBlockSize_{initialBlockSize == 0 ? DEFAULT_INITIAL_BLOCK_SIZE : initialBlockSize},
          nextBlockSize_{initialBlockSize_},
          maxBlockSize_{std::max(maxBlockSize, initialBlockSize_)},
          head_{nullptr},
          tail_{nullptr},
          current_{nullptr} {}

    BufferArena::~BufferArena() noexcept { clear(); }

    std::byte* BufferArena::allocate(size_t size, size_t align) {
        if (size == 0) { return nullptr; }
        if (align == 0) { align = 1; }
        if (!isPowerOfTwo(align)) { throw std::invalid_argument("BufferArena::allocate: alignment must be power-of-two"); }

        if (current_ != nullptr) { if (auto* ptr = tryAllocateFromBlock(current_, size, align); ptr != nullptr) { return ptr; } }

        if (size > (static_cast<size_t>(-1) - (align - 1))) { throw std::bad_alloc(); }
        const size_t minCapacity = size + (align - 1);
        const size_t desiredCapacity = std::max(nextBlockSize_, minCapacity);
        const size_t blockCapacity = std::min(std::max(desiredCapacity, initialBlockSize_), maxBlockSize_);
        const size_t blockAlignment = std::max(align, alignof(std::max_align_t));

        Block* block = createBlock(blockCapacity >= minCapacity ? blockCapacity : minCapacity, blockAlignment);
        if (head_ == nullptr) {
            head_ = block;
            tail_ = block;
        }
        else {
            tail_->next = block;
            tail_ = block;
        }
        current_ = block;

        if (nextBlockSize_ < maxBlockSize_) {
            size_t doubled = nextBlockSize_ * 2;
            if (doubled < nextBlockSize_) doubled = maxBlockSize_; // overflow guard
            nextBlockSize_ = std::min(doubled, maxBlockSize_);
        }

        auto* ptr = tryAllocateFromBlock(current_, size, align);
        if (ptr == nullptr) { throw std::bad_alloc(); }
        return ptr;
    }

    void BufferArena::reset() noexcept {
        for (Block* b = head_; b != nullptr; b = b->next) { b->offset = 0; }
        current_ = head_;
    }

    void BufferArena::clear() noexcept {
        Block* b = head_;
        while (b != nullptr) {
            Block* next = b->next;
            operator delete(b->data, static_cast<std::align_val_t>(b->alignment));
            delete b;
            b = next;
        }

        head_ = nullptr;
        tail_ = nullptr;
        current_ = nullptr;
        nextBlockSize_ = initialBlockSize_;
    }

    bool BufferArena::isPowerOfTwo(size_t x) noexcept { return x != 0 && (x & (x - 1)) == 0; }

    size_t BufferArena::alignUp(size_t x, size_t align) noexcept { return (x + (align - 1)) & ~(align - 1); }

    BufferArena::Block* BufferArena::createBlock(size_t capacity, size_t alignment) {
        auto* block = new Block{};
        block->data = static_cast<std::byte*>(operator new(capacity, static_cast<std::align_val_t>(alignment)));
        block->capacity = capacity;
        block->offset = 0;
        block->alignment = alignment;
        block->next = nullptr;
        return block;
    }

    std::byte* BufferArena::tryAllocateFromBlock(Block* block, size_t size, size_t align) noexcept {
        const size_t aligned = alignUp(block->offset, align);
        if (aligned > block->capacity || size > block->capacity - aligned) { return nullptr; }

        auto* ptr = block->data + aligned;
        block->offset = aligned + size;
        return ptr;
    }
} // namespace akkaradb::core

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
    BufferArena::BufferArena(size_t initial_block_size, size_t max_block_size)
        : initial_block_size_{initial_block_size == 0 ? DEFAULT_INITIAL_BLOCK_SIZE : initial_block_size},
          next_block_size_{initial_block_size_},
          max_block_size_{std::max(max_block_size, initial_block_size_)},
          head_{nullptr},
          tail_{nullptr},
          current_{nullptr} {}

    BufferArena::~BufferArena() noexcept { clear(); }

    std::byte* BufferArena::allocate(size_t size, size_t align) {
        if (size == 0) { return nullptr; }
        if (align == 0) { align = 1; }
        if (!is_power_of_two(align)) { throw std::invalid_argument("BufferArena::allocate: alignment must be power-of-two"); }

        if (current_ != nullptr) { if (auto* ptr = try_allocate_from_block(current_, size, align); ptr != nullptr) { return ptr; } }

        if (size > (static_cast<size_t>(-1) - (align - 1))) { throw std::bad_alloc(); }
        const size_t min_capacity = size + (align - 1);
        const size_t desired_capacity = std::max(next_block_size_, min_capacity);
        const size_t block_capacity = std::min(std::max(desired_capacity, initial_block_size_), max_block_size_);
        const size_t block_alignment = std::max(align, alignof(std::max_align_t));

        Block* block = create_block(block_capacity >= min_capacity ? block_capacity : min_capacity, block_alignment);
        if (head_ == nullptr) {
            head_ = block;
            tail_ = block;
        }
        else {
            tail_->next = block;
            tail_ = block;
        }
        current_ = block;

        if (next_block_size_ < max_block_size_) {
            size_t doubled = next_block_size_ * 2;
            if (doubled < next_block_size_) doubled = max_block_size_; // overflow guard
            next_block_size_ = std::min(doubled, max_block_size_);
        }

        auto* ptr = try_allocate_from_block(current_, size, align);
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
        next_block_size_ = initial_block_size_;
    }

    bool BufferArena::is_power_of_two(size_t x) noexcept { return x != 0 && (x & (x - 1)) == 0; }

    size_t BufferArena::align_up(size_t x, size_t align) noexcept { return (x + (align - 1)) & ~(align - 1); }

    BufferArena::Block* BufferArena::create_block(size_t capacity, size_t alignment) {
        auto* block = new Block{};
        block->data = static_cast<std::byte*>(operator new(capacity, static_cast<std::align_val_t>(alignment)));
        block->capacity = capacity;
        block->offset = 0;
        block->alignment = alignment;
        block->next = nullptr;
        return block;
    }

    std::byte* BufferArena::try_allocate_from_block(Block* block, size_t size, size_t align) noexcept {
        const size_t aligned = align_up(block->offset, align);
        if (aligned > block->capacity || size > block->capacity - aligned) { return nullptr; }

        auto* ptr = block->data + aligned;
        block->offset = aligned + size;
        return ptr;
    }
} // namespace akkaradb::core

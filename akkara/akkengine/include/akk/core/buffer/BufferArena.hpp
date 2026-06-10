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

// akkengine/include/akk/core/buffer/BufferArena.hpp
#pragma once

#include <cstddef>

namespace akkaradb::core {
    /**
     * @brief Monotonic bump allocator for transient byte buffers.
     *
     * Characteristics:
     * - allocate(size, align) only
     * - no per-allocation free
     * - reset() rewinds all blocks in O(numBlocks)
     * - thread-unsafe by design
     *
     * Intended usage:
     * - temporary buffers during request-local / stage-local processing
     * - memory is invalidated on reset()/destruction
     */
    class BufferArena {
        public:
            static constexpr size_t DEFAULT_INITIAL_BLOCK_SIZE = 64 * 1024;
            static constexpr size_t DEFAULT_MAX_BLOCK_SIZE = 4 * 1024 * 1024;

            explicit BufferArena(size_t initialBlockSize = DEFAULT_INITIAL_BLOCK_SIZE, size_t maxBlockSize = DEFAULT_MAX_BLOCK_SIZE);

            ~BufferArena() noexcept;

            BufferArena(const BufferArena&) = delete;
            BufferArena& operator=(const BufferArena&) = delete;
            BufferArena(BufferArena&&) = delete;
            BufferArena& operator=(BufferArena&&) = delete;

            /**
             * @brief Allocates aligned memory from the arena.
             *
             * @param size Requested size in bytes.
             * @param align Power-of-two alignment (default: max_align_t).
             * @return Pointer to writable bytes, or nullptr when size == 0.
             */
            [[nodiscard]] std::byte* allocate(size_t size, size_t align = alignof(std::max_align_t));

            /**
             * @brief Rewinds all blocks for reuse.
             *
             * Memory from previous allocations becomes invalid.
             */
            void reset() noexcept;

            /**
             * @brief Releases all blocks owned by this arena.
             */
            void clear() noexcept;

        private:
            struct Block {
                std::byte* data;
                size_t capacity;
                size_t offset;
                size_t alignment;
                Block* next;
            };

            size_t initialBlockSize_;
            size_t nextBlockSize_;
            size_t maxBlockSize_;
            Block* head_;
            Block* tail_;
            Block* current_;

            [[nodiscard]] static bool isPowerOfTwo(size_t x) noexcept;
            [[nodiscard]] static size_t alignUp(size_t x, size_t align) noexcept;
            [[nodiscard]] Block* createBlock(size_t capacity, size_t alignment);
            [[nodiscard]] static std::byte* tryAllocateFromBlock(Block* block, size_t size, size_t align) noexcept;
    };
} // namespace akkaradb::core

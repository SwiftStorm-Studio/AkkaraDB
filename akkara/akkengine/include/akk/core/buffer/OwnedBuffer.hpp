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

// akkengine/include/akk/core/buffer/OwnedBuffer.hpp
#pragma once

#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <utility>

namespace akkaradb::core {
    class BufferView;

    /**
     * @brief Owning byte buffer with customizable deallocation strategy.
     *
     * OwnedBuffer represents a contiguous memory region with exclusive ownership.
     * It is a move-only type that releases its memory using a user-provided
     * deleter function when destroyed.
     *
     * Design principles:
     * - Exclusive ownership (move-only, no copying)
     * - Pluggable deallocation strategy (heap / pool / arena / custom)
     * - Zero-overhead abstraction (no virtual dispatch, no heap for control block)
     * - Interoperable with BufferView (non-owning view)
     *
     * Lifetime model:
     * - The buffer is released when the OwnedBuffer is destroyed
     * - The release mechanism depends on the configured deleter
     *
     * Example:
     * @code
     * auto buf = OwnedBuffer::allocate(1024); // heap allocation
     * auto view = buf.as_view();              // non-owning view
     * @endcode
     *
     * Custom allocator example (pool):
     * @code
     * auto buf = OwnedBuffer(ptr, size, pool_deleter, pool_ptr);
     * @endcode
     *
     * Thread-safety:
     * - Not thread-safe for concurrent mutation
     * - Safe to move across threads
     */
    class OwnedBuffer {
        public:
            /**
             * @brief Deleter function signature.
             *
             * @param ptr Pointer to buffer
             * @param size Size of buffer
             * @param ctx User-defined context (allocator / pool / arena)
             */
            typedef void (*Deleter)(void* ptr, size_t size, void* ctx);

            // ==================== Constructors ====================

            /**
             * @brief Constructs an empty buffer.
             */
            constexpr OwnedBuffer() noexcept = default;

            /**
             * @brief Constructs a buffer with explicit ownership and deleter.
             *
             * @param data Pointer to memory
             * @param size Size in bytes
             * @param deleter Deallocation function
             * @param ctx Context passed to deleter
             *
             * @warning data must be valid for the given size.
             */
            OwnedBuffer(std::byte* data, size_t size, Deleter deleter, void* ctx) noexcept : data_{data}, size_{size}, deleter_{deleter}, ctx_{ctx} {}

            // ==================== Move semantics ====================

            OwnedBuffer(const OwnedBuffer&) = delete;
            OwnedBuffer& operator=(const OwnedBuffer&) = delete;

            OwnedBuffer(OwnedBuffer&& other) noexcept
                : data_{other.data_}, size_{other.size_}, deleter_{other.deleter_}, ctx_{other.ctx_} {
                other.data_ = nullptr;
                other.size_ = 0;
                other.deleter_ = nullptr;
                other.ctx_ = nullptr;
            }

            OwnedBuffer& operator=(OwnedBuffer&& other) noexcept {
                if (this != &other) {
                    reset();

                    data_ = other.data_;
                    size_ = other.size_;
                    deleter_ = other.deleter_;
                    ctx_ = other.ctx_;

                    other.data_ = nullptr;
                    other.size_ = 0;
                    other.deleter_ = nullptr;
                    other.ctx_ = nullptr;
                }
                return *this;
            }

            // ==================== Destructor ====================

            /**
             * @brief Releases the owned buffer using the configured deleter.
             */
            ~OwnedBuffer() { reset(); }

            /**
             * @brief Releases ownership and invokes the deleter.
             */
            void reset() noexcept {
                if (data_) [[likely]] { deleter_(data_, size_, ctx_); }
                data_ = nullptr;
                size_ = 0;
                deleter_ = nullptr;
                ctx_ = nullptr;
            }

            // ==================== Factory ====================

            /**
             * @brief Allocates a buffer on the heap.
             *
             * Uses global operator new/delete.
             *
             * @param size Size in bytes
             * @return OwnedBuffer instance
             */
            static OwnedBuffer allocate(size_t size);

            // ==================== Accessors ====================

            /**
             * @brief Returns raw pointer.
             */
            [[nodiscard]] std::byte* data() noexcept { return data_; }

            /**
             * @brief Returns raw pointer (const).
             */
            [[nodiscard]] const std::byte* data() const noexcept { return data_; }

            /**
             * @brief Returns buffer size in bytes.
             */
            [[nodiscard]] size_t size() const noexcept { return size_; }

            /**
             * @brief Returns whether buffer is empty.
             */
            [[nodiscard]] bool empty() const noexcept { return size_ == 0; }

            /**
             * @brief Creates a non-owning view of the buffer.
             */
            [[nodiscard]] BufferView as_view() const noexcept;

            // ==================== Ownership control ====================

            /**
             * @brief Swaps with another buffer.
             */
            void swap(OwnedBuffer& other) noexcept {
                std::swap(data_, other.data_);
                std::swap(size_, other.size_);
                std::swap(deleter_, other.deleter_);
                std::swap(ctx_, other.ctx_);
            }

        private:
            std::byte* data_ = nullptr;
            size_t size_ = 0;
            Deleter deleter_ = nullptr;
            void* ctx_ = nullptr;
    };

    static_assert(!std::is_copy_constructible_v<OwnedBuffer>);
    static_assert(std::is_move_constructible_v<OwnedBuffer>);
} // namespace akkaradb::core

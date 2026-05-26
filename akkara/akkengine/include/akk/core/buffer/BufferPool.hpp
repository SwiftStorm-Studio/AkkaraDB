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

// akkengine/include/akk/core/buffer/BufferPool.hpp
#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>

namespace akkaradb::core {
    class OwnedBuffer;

    /**
     * @brief Reusable variable-size byte buffer pool (size-class based).
     *
     * Size classes are powers of two in [min_class_size, max_class_size].
     * Requests beyond max_class_size fall back to heap allocation.
     *
     * Fast path:
     * - Optional thread-local cache (single active pool per thread)
     * - No dynamic allocation on acquire/release
     *
     * Thread-safety:
     * - Safe for concurrent acquire/release
     * - Pool should outlive worker threads when TLS cache is enabled
     */
    class BufferPool {
        public:
            static constexpr size_t DEFAULT_MIN_CLASS_SIZE = 64;
            static constexpr size_t DEFAULT_MAX_CLASS_SIZE = 64 * 1024;
            static constexpr size_t DEFAULT_TLS_CACHE_LIMIT = 16;

            explicit BufferPool(
                size_t min_class_size = DEFAULT_MIN_CLASS_SIZE,
                size_t max_class_size = DEFAULT_MAX_CLASS_SIZE,
                size_t tls_cache_limit = DEFAULT_TLS_CACHE_LIMIT
            );

            ~BufferPool() noexcept;

            BufferPool(const BufferPool&) = delete;
            BufferPool& operator=(const BufferPool&) = delete;
            BufferPool(BufferPool&&) = delete;
            BufferPool& operator=(BufferPool&&) = delete;

            /**
             * @brief Allocates a pool-backed owned buffer.
             *
             * @param size Requested size in bytes.
             * @return OwnedBuffer that returns memory to this pool on destruction.
             */
            [[nodiscard]] OwnedBuffer allocate(size_t size);

        private:
            static constexpr uint32_t HEAP_CLASS_INDEX = std::numeric_limits<uint32_t>::max();

            struct Header {
                union {
                    uint32_t class_index;
                    std::max_align_t align_guard;
                };
            };

            static_assert(alignof(Header) >= alignof(std::max_align_t), "Header alignment must satisfy max_align_t");
            static_assert(sizeof(Header) >= sizeof(std::max_align_t), "Header size must be at least max_align_t");

            struct FreeNode {
                FreeNode* next;
            };

            static_assert(sizeof(FreeNode) <= DEFAULT_MIN_CLASS_SIZE, "FreeNode must fit into the minimum size class payload");

            struct SizeClass {
                std::mutex mutex;
                FreeNode* head = nullptr;
            };

            size_t min_class_size_;
            size_t max_class_size_;
            size_t tls_cache_limit_;
            size_t class_count_;
            std::unique_ptr<SizeClass[]> classes_;

            [[nodiscard]] static bool is_power_of_two(size_t x) noexcept;
            [[nodiscard]] static size_t ceil_pow2(size_t x) noexcept;
            [[nodiscard]] int class_index_for(size_t size) const noexcept;
            [[nodiscard]] size_t class_size_for(int class_index) const noexcept;

            [[nodiscard]] std::byte* acquire_raw(size_t size);
            void release_raw(void* ptr) noexcept;

            static void owned_deleter(void* ptr, size_t size, void* ctx) noexcept;

            [[nodiscard]] FreeNode* pop_global(int class_index) noexcept;
            void push_global(int class_index, FreeNode* node) noexcept;
    };
} // namespace akkaradb::core

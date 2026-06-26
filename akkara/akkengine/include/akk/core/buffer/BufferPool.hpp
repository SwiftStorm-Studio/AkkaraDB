/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/core/buffer/BufferPool.hpp
#pragma once

#include "akkaradb/Export.hpp"

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
     * Size classes are powers of two in [minClassSize, maxClassSize].
     * Requests beyond maxClassSize fall back to heap allocation.
     *
     * Fast path:
     * - Optional thread-local cache (single active pool per thread)
     * - No dynamic allocation on acquire/release
     *
     * Thread-safety:
     * - Safe for concurrent acquire/release
     * - Pool should outlive worker threads when TLS cache is enabled
     */
    class AKDB_API BufferPool {
        public:
            static constexpr size_t DEFAULT_MIN_CLASS_SIZE = 64;
            static constexpr size_t DEFAULT_MAX_CLASS_SIZE = 64 * 1024;
            static constexpr size_t DEFAULT_TLS_CACHE_LIMIT = 16;

            explicit BufferPool(
                size_t minClassSize = DEFAULT_MIN_CLASS_SIZE,
                size_t maxClassSize = DEFAULT_MAX_CLASS_SIZE,
                size_t tlsCacheLimit = DEFAULT_TLS_CACHE_LIMIT
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
                    uint32_t classIndex;
                    std::max_align_t alignGuard;
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

            size_t minClassSize_;
            size_t maxClassSize_;
            size_t tlsCacheLimit_;
            size_t classCount_;
            std::unique_ptr<SizeClass[]> classes_;

            [[nodiscard]] static bool isPowerOfTwo(size_t x) noexcept;
            [[nodiscard]] static size_t ceilPow2(size_t x) noexcept;
            [[nodiscard]] int classIndexFor(size_t size) const noexcept;
            [[nodiscard]] size_t classSizeFor(int classIndex) const noexcept;

            [[nodiscard]] std::byte* acquireRaw(size_t size);
            void releaseRaw(void* ptr) noexcept;

            static void ownedDeleter(void* ptr, size_t size, void* ctx) noexcept;

            [[nodiscard]] FreeNode* popGlobal(int classIndex) noexcept;
            void pushGlobal(int classIndex, FreeNode* node) noexcept;
    };
} // namespace akkaradb::core

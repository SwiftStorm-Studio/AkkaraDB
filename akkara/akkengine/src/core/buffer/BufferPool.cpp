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

// akkengine/src/core/buffer/BufferPool.cpp
#include "akk/core/buffer/BufferPool.hpp"

#include <bit>

#include "akk/core/buffer/OwnedBuffer.hpp"

#include <new>
#include <stdexcept>
#include <utility>
#include <vector>

namespace akkaradb::core {
    namespace {
        constexpr size_t ALLOC_ALIGNMENT = alignof(std::max_align_t);
        constexpr size_t TLS_MAX_CLASSES = 32;
        constexpr size_t TLS_MAX_ENTRIES_PER_CLASS = 64;
        constexpr size_t TLS_MAX_POOLS_PER_THREAD = 8;

        struct ThreadLocalEntry {
            void* raw = nullptr;
            std::byte* payload = nullptr;
        };

        struct ThreadLocalClassCache {
            ThreadLocalEntry entries[TLS_MAX_ENTRIES_PER_CLASS]{};
            uint8_t count{0};
        };

        struct ThreadLocalPoolCache {
            const BufferPool* pool{nullptr};
            size_t class_count{0};
            std::unique_ptr<ThreadLocalClassCache[]> bins{};

            ThreadLocalPoolCache() = default;
            ThreadLocalPoolCache(const ThreadLocalPoolCache&) = delete;
            ThreadLocalPoolCache& operator=(const ThreadLocalPoolCache&) = delete;
            ThreadLocalPoolCache(ThreadLocalPoolCache&&) noexcept = default;
            ThreadLocalPoolCache& operator=(ThreadLocalPoolCache&&) noexcept = default;

            void clear() noexcept {
                if (!bins) { return; }
                for (size_t i = 0; i < class_count; ++i) {
                    auto& [entries, count] = bins[i];
                    while (count > 0) {
                        auto& [raw, payload] = entries[--count];
                        operator delete(raw, static_cast<std::align_val_t>(ALLOC_ALIGNMENT));
                        raw = nullptr;
                        payload = nullptr;
                    }
                }
            }

            ~ThreadLocalPoolCache() { clear(); }
        };

        thread_local std::vector<ThreadLocalPoolCache> tls_pools;

        ThreadLocalPoolCache* find_tls_pool_cache(const BufferPool* pool) noexcept {
            for (auto& cache : tls_pools) { if (cache.pool == pool) { return &cache; } }
            return nullptr;
        }

        ThreadLocalPoolCache* get_or_create_tls_pool_cache(const BufferPool* pool, size_t class_count) {
            if (auto* found = find_tls_pool_cache(pool); found != nullptr) { return found; }
            if (tls_pools.size() >= TLS_MAX_POOLS_PER_THREAD) { return nullptr; }

            ThreadLocalPoolCache cache;
            cache.pool = pool;
            cache.class_count = class_count;
            cache.bins = std::make_unique<ThreadLocalClassCache[]>(class_count);
            tls_pools.push_back(std::move(cache));
            return &tls_pools.back();
        }

        void erase_tls_pool_cache_for_current_thread(const BufferPool* pool) noexcept {
            for (size_t i = 0; i < tls_pools.size(); ++i) {
                if (tls_pools[i].pool == pool) {
                    if (i + 1 != tls_pools.size()) { std::swap(tls_pools[i], tls_pools.back()); }
                    tls_pools.pop_back();
                    return;
                }
            }
        }
    } // namespace

    BufferPool::BufferPool(size_t min_class_size, size_t max_class_size, size_t tls_cache_limit)
        : min_class_size_{ceil_pow2(min_class_size < sizeof(FreeNode) ? sizeof(FreeNode) : min_class_size)},
          max_class_size_{ceil_pow2(max_class_size < min_class_size_ ? min_class_size_ : max_class_size)},
          tls_cache_limit_{tls_cache_limit},
          class_count_{0} {
        if (tls_cache_limit_ > TLS_MAX_ENTRIES_PER_CLASS) { tls_cache_limit_ = TLS_MAX_ENTRIES_PER_CLASS; }

        size_t size = min_class_size_;
        while (size <= max_class_size_) {
            ++class_count_;
            if (size > (static_cast<size_t>(-1) >> 1)) { break; }
            size <<= 1;
        }

        if (class_count_ == 0 || class_count_ > TLS_MAX_CLASSES) {
            throw std::invalid_argument("BufferPool: invalid size class configuration");
        }

        classes_ = std::make_unique<SizeClass[]>(class_count_);
    }

    BufferPool::~BufferPool() noexcept {
        // Only this thread's TLS cache is directly reachable here.
        // Other threads keep independent TLS caches and release their cached blocks
        // to heap on thread exit (ThreadLocalPoolCache::~ThreadLocalPoolCache).
        erase_tls_pool_cache_for_current_thread(this);

        for (size_t i = 0; i < class_count_; ++i) {
            std::lock_guard lock(classes_[i].mutex);
            auto* node = classes_[i].head;
            while (node != nullptr) {
                auto* next = node->next;
                auto* header = reinterpret_cast<Header*>(node) - 1;
                operator delete(header, static_cast<std::align_val_t>(alignof(Header)));
                node = next;
            }
            classes_[i].head = nullptr;
        }
    }

    std::byte* BufferPool::acquire_raw(size_t size) {
        if (size == 0) { return nullptr; }

        const int idx = class_index_for(size);
        if (idx < 0) {
            auto* raw = static_cast<std::byte*>(operator new(sizeof(Header) + size, static_cast<std::align_val_t>(alignof(Header))));
            auto* h = reinterpret_cast<Header*>(raw);
            h->class_index = HEAP_CLASS_INDEX;
            return reinterpret_cast<std::byte*>(h + 1);
        }

        if (tls_cache_limit_ != 0) {
            if (auto* cache = get_or_create_tls_pool_cache(this, class_count_); cache != nullptr) {
                auto& [entries, count] = cache->bins[idx];
                if (count > 0) {
                    auto& [raw, payload] = entries[--count];
                    raw = nullptr;
                    return payload;
                }
            }
        }

        if (auto* node = pop_global(idx); node != nullptr) { return reinterpret_cast<std::byte*>(node); }

        const size_t class_size = class_size_for(idx);
        auto* raw = static_cast<std::byte*>(operator new(sizeof(Header) + class_size, static_cast<std::align_val_t>(alignof(Header))));
        auto* h = reinterpret_cast<Header*>(raw);
        h->class_index = static_cast<uint32_t>(idx);
        return reinterpret_cast<std::byte*>(h + 1);
    }

    void BufferPool::release_raw(void* ptr) noexcept {
        if (ptr == nullptr) { return; }

        auto* h = static_cast<Header*>(ptr) - 1;
        const uint32_t class_index = h->class_index;
        if (class_index == HEAP_CLASS_INDEX) {
            operator delete(h, static_cast<std::align_val_t>(alignof(Header)));
            return;
        }

        const int idx = static_cast<int>(class_index);
        if (idx < 0 || static_cast<size_t>(idx) >= class_count_) {
            operator delete(h, static_cast<std::align_val_t>(alignof(Header)));
            return;
        }

        auto* payload = static_cast<std::byte*>(ptr);
        auto* node = reinterpret_cast<FreeNode*>(payload);

        if (tls_cache_limit_ != 0) {
            if (auto* cache = get_or_create_tls_pool_cache(this, class_count_); cache != nullptr) {
                auto& bin = cache->bins[idx];
                if (bin.count < tls_cache_limit_) {
                    auto& [raw, payload_ref] = bin.entries[bin.count++];
                    raw = h;
                    payload_ref = payload;
                    return;
                }

                size_t spill = tls_cache_limit_ / 2;
                if (spill == 0) { spill = 1; }
                while (spill-- > 0 && bin.count > 0) {
                    auto& [raw, spill_payload] = bin.entries[--bin.count];
                    auto* spill_node = reinterpret_cast<FreeNode*>(spill_payload);
                    raw = nullptr;
                    spill_payload = nullptr;
                    push_global(idx, spill_node);
                }

                auto& [raw, payload_ref] = bin.entries[bin.count++];
                raw = h;
                payload_ref = payload;
                return;
            }
        }

        push_global(idx, node);
    }

    OwnedBuffer BufferPool::allocate(size_t size) {
        if (size == 0) { return OwnedBuffer{}; }
        return OwnedBuffer{acquire_raw(size), size, &BufferPool::owned_deleter, this};
    }

    void BufferPool::owned_deleter(void* ptr, size_t size, void* ctx) noexcept {
        (void)size;
        if (ptr == nullptr) { return; }

        if (ctx == nullptr) {
            auto* h = static_cast<Header*>(ptr) - 1;
            operator delete(h, static_cast<std::align_val_t>(alignof(Header)));
            return;
        }

        static_cast<BufferPool*>(ctx)->release_raw(ptr);
    }

    bool BufferPool::is_power_of_two(size_t x) noexcept { return x != 0 && (x & (x - 1)) == 0; }

    size_t BufferPool::ceil_pow2(size_t x) noexcept {
        if (x <= 1) { return 1; }

        --x;
        x |= x >> 1;
        x |= x >> 2;
        x |= x >> 4;
        x |= x >> 8;
        x |= x >> 16;
        #if SIZE_MAX > UINT32_MAX
        x |= x >> 32;
        #endif
        return x + 1;
    }

    int BufferPool::class_index_for(size_t size) const noexcept {
        if (size == 0 || size > max_class_size_) { return -1; }

        const size_t v = ceil_pow2(size);
        const unsigned log2_v = static_cast<unsigned>(std::bit_width(v) - 1);
        const unsigned log2_min = static_cast<unsigned>(std::bit_width(min_class_size_) - 1);

        return static_cast<int>(log2_v - log2_min);
    }

    size_t BufferPool::class_size_for(int class_index) const noexcept { return min_class_size_ << class_index; }

    BufferPool::FreeNode* BufferPool::pop_global(int class_index) noexcept {
        auto& [mutex, head] = classes_[class_index];
        std::lock_guard lock(mutex);

        auto* node = head;
        if (node != nullptr) {
            head = node->next;
            node->next = nullptr;
        }
        return node;
    }

    void BufferPool::push_global(int class_index, FreeNode* node) noexcept {
        auto& [mutex, head] = classes_[class_index];
        std::lock_guard lock(mutex);
        node->next = head;
        head = node;
    }
} // namespace akkaradb::core

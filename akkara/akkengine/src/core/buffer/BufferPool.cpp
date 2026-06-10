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
            size_t classCount{0};
            std::unique_ptr<ThreadLocalClassCache[]> bins{};

            ThreadLocalPoolCache() = default;
            ThreadLocalPoolCache(const ThreadLocalPoolCache&) = delete;
            ThreadLocalPoolCache& operator=(const ThreadLocalPoolCache&) = delete;
            ThreadLocalPoolCache(ThreadLocalPoolCache&&) noexcept = default;
            ThreadLocalPoolCache& operator=(ThreadLocalPoolCache&&) noexcept = default;

            void clear() noexcept {
                if (!bins) { return; }
                for (size_t i = 0; i < classCount; ++i) {
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

        thread_local std::vector<ThreadLocalPoolCache> tlsPools;

        ThreadLocalPoolCache* findTlsPoolCache(const BufferPool* pool) noexcept {
            for (auto& cache : tlsPools) { if (cache.pool == pool) { return &cache; } }
            return nullptr;
        }

        ThreadLocalPoolCache* getOrCreateTlsPoolCache(const BufferPool* pool, size_t classCount) {
            if (auto* found = findTlsPoolCache(pool); found != nullptr) { return found; }
            if (tlsPools.size() >= TLS_MAX_POOLS_PER_THREAD) { return nullptr; }

            ThreadLocalPoolCache cache;
            cache.pool = pool;
            cache.classCount = classCount;
            cache.bins = std::make_unique<ThreadLocalClassCache[]>(classCount);
            tlsPools.push_back(std::move(cache));
            return &tlsPools.back();
        }

        void eraseTlsPoolCacheForCurrentThread(const BufferPool* pool) noexcept {
            for (size_t i = 0; i < tlsPools.size(); ++i) {
                if (tlsPools[i].pool == pool) {
                    if (i + 1 != tlsPools.size()) { std::swap(tlsPools[i], tlsPools.back()); }
                    tlsPools.pop_back();
                    return;
                }
            }
        }
    } // namespace

    BufferPool::BufferPool(size_t minClassSize, size_t maxClassSize, size_t tlsCacheLimit)
        : minClassSize_{ceilPow2(minClassSize < sizeof(FreeNode) ? sizeof(FreeNode) : minClassSize)},
          maxClassSize_{ceilPow2(maxClassSize < minClassSize_ ? minClassSize_ : maxClassSize)},
          tlsCacheLimit_{tlsCacheLimit},
          classCount_{0} {
        if (tlsCacheLimit_ > TLS_MAX_ENTRIES_PER_CLASS) { tlsCacheLimit_ = TLS_MAX_ENTRIES_PER_CLASS; }

        size_t size = minClassSize_;
        while (size <= maxClassSize_) {
            ++classCount_;
            if (size > (static_cast<size_t>(-1) >> 1)) { break; }
            size <<= 1;
        }

        if (classCount_ == 0 || classCount_ > TLS_MAX_CLASSES) {
            throw std::invalid_argument("BufferPool: invalid size class configuration");
        }

        classes_ = std::make_unique<SizeClass[]>(classCount_);
    }

    BufferPool::~BufferPool() noexcept {
        // Only this thread's TLS cache is directly reachable here.
        // Other threads keep independent TLS caches and release their cached blocks
        // to heap on thread exit (ThreadLocalPoolCache::~ThreadLocalPoolCache).
        eraseTlsPoolCacheForCurrentThread(this);

        for (size_t i = 0; i < classCount_; ++i) {
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

    std::byte* BufferPool::acquireRaw(size_t size) {
        if (size == 0) { return nullptr; }

        const int idx = classIndexFor(size);
        if (idx < 0) {
            auto* raw = static_cast<std::byte*>(operator new(sizeof(Header) + size, static_cast<std::align_val_t>(alignof(Header))));
            auto* h = reinterpret_cast<Header*>(raw);
            h->classIndex = HEAP_CLASS_INDEX;
            return reinterpret_cast<std::byte*>(h + 1);
        }

        if (tlsCacheLimit_ != 0) {
            if (auto* cache = getOrCreateTlsPoolCache(this, classCount_); cache != nullptr) {
                auto& [entries, count] = cache->bins[idx];
                if (count > 0) {
                    auto& [raw, payload] = entries[--count];
                    raw = nullptr;
                    return payload;
                }
            }
        }

        if (auto* node = popGlobal(idx); node != nullptr) { return reinterpret_cast<std::byte*>(node); }

        const size_t classSize = classSizeFor(idx);
        auto* raw = static_cast<std::byte*>(operator new(sizeof(Header) + classSize, static_cast<std::align_val_t>(alignof(Header))));
        auto* h = reinterpret_cast<Header*>(raw);
        h->classIndex = static_cast<uint32_t>(idx);
        return reinterpret_cast<std::byte*>(h + 1);
    }

    void BufferPool::releaseRaw(void* ptr) noexcept {
        if (ptr == nullptr) { return; }

        auto* h = static_cast<Header*>(ptr) - 1;
        const uint32_t classIndex = h->classIndex;
        if (classIndex == HEAP_CLASS_INDEX) {
            operator delete(h, static_cast<std::align_val_t>(alignof(Header)));
            return;
        }

        const int idx = static_cast<int>(classIndex);
        if (idx < 0 || static_cast<size_t>(idx) >= classCount_) {
            operator delete(h, static_cast<std::align_val_t>(alignof(Header)));
            return;
        }

        auto* payload = static_cast<std::byte*>(ptr);
        auto* node = reinterpret_cast<FreeNode*>(payload);

        if (tlsCacheLimit_ != 0) {
            if (auto* cache = getOrCreateTlsPoolCache(this, classCount_); cache != nullptr) {
                auto& bin = cache->bins[idx];
                if (bin.count < tlsCacheLimit_) {
                    auto& [raw, payloadRef] = bin.entries[bin.count++];
                    raw = h;
                    payloadRef = payload;
                    return;
                }

                size_t spill = tlsCacheLimit_ / 2;
                if (spill == 0) { spill = 1; }
                while (spill-- > 0 && bin.count > 0) {
                    auto& [raw, spillPayload] = bin.entries[--bin.count];
                    auto* spillNode = reinterpret_cast<FreeNode*>(spillPayload);
                    raw = nullptr;
                    spillPayload = nullptr;
                    pushGlobal(idx, spillNode);
                }

                auto& [raw, payloadRef] = bin.entries[bin.count++];
                raw = h;
                payloadRef = payload;
                return;
            }
        }

        pushGlobal(idx, node);
    }

    OwnedBuffer BufferPool::allocate(size_t size) {
        if (size == 0) { return OwnedBuffer{}; }
        return OwnedBuffer{acquireRaw(size), size, &BufferPool::ownedDeleter, this};
    }

    void BufferPool::ownedDeleter(void* ptr, size_t size, void* ctx) noexcept {
        (void)size;
        if (ptr == nullptr) { return; }

        if (ctx == nullptr) {
            auto* h = static_cast<Header*>(ptr) - 1;
            operator delete(h, static_cast<std::align_val_t>(alignof(Header)));
            return;
        }

        static_cast<BufferPool*>(ctx)->releaseRaw(ptr);
    }

    bool BufferPool::isPowerOfTwo(size_t x) noexcept { return x != 0 && (x & (x - 1)) == 0; }

    size_t BufferPool::ceilPow2(size_t x) noexcept {
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

    int BufferPool::classIndexFor(size_t size) const noexcept {
        if (size == 0 || size > maxClassSize_) { return -1; }

        const size_t v = ceilPow2(size);
        const unsigned log2V = static_cast<unsigned>(std::bit_width(v) - 1);
        const unsigned log2Min = static_cast<unsigned>(std::bit_width(minClassSize_) - 1);

        return static_cast<int>(log2V - log2Min);
    }

    size_t BufferPool::classSizeFor(int classIndex) const noexcept { return minClassSize_ << classIndex; }

    BufferPool::FreeNode* BufferPool::popGlobal(int classIndex) noexcept {
        auto& [mutex, head] = classes_[classIndex];
        std::lock_guard lock(mutex);

        auto* node = head;
        if (node != nullptr) {
            head = node->next;
            node->next = nullptr;
        }
        return node;
    }

    void BufferPool::pushGlobal(int classIndex, FreeNode* node) noexcept {
        auto& [mutex, head] = classes_[classIndex];
        std::lock_guard lock(mutex);
        node->next = head;
        head = node;
    }
} // namespace akkaradb::core

/*
 * AkkaraDB
 * Copyright (C) 2026 Swift Storm Studio
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. See https://mozilla.org/MPL/2.0/.
 */
#pragma once

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <span>
#include <stdexcept>
#include <vector>

#include "akk/engine/memtable/detail/WormholeHash.hpp"

#if defined(_M_X64) || defined(__SSE2__)
#include <emmintrin.h>
#endif

namespace akkaradb::engine::memtable::detail::wormhole {
    inline uint64_t finishHash(uint64_t hash, size_t length) noexcept {
        hash ^= static_cast<uint64_t>(length) * 0x9e3779b97f4a7c15ULL;
        hash ^= hash >> 33;
        hash *= 0xff51afd7ed558ccdULL;
        hash ^= hash >> 33;
        hash *= 0xc4ceb9fe1a85ec53ULL;
        return hash ^ (hash >> 33);
    }
    inline uint16_t tagOf(uint64_t hash) noexcept {
        const auto tag = static_cast<uint16_t>(hash >> 48);
        return tag == 0 ? uint16_t{1} : tag;
    }

    // The separate tail permits predecessor-child probes without copying keys.
    struct PrefixKey {
        const uint8_t* data;
        uint64_t hash;
        uint32_t length;
        uint8_t tail;
    };
    inline bool equalPrefix(const PrefixKey& a, const PrefixKey& b) noexcept {
        return a.hash == b.hash && a.length == b.length && (a.length == 0 ||
            (a.tail == b.tail && (a.length == 1 || std::memcmp(a.data, b.data, a.length - 1) == 0)));
    }
    inline PrefixKey prefix(std::span<const uint8_t> key, size_t size, uint64_t hash) noexcept {
        return {key.data(), finishHash(hash, size), static_cast<uint32_t>(size), size == 0 ? uint8_t{0} : key[size - 1]};
    }

    // Eight tags are checked without dereferencing metadata. Full-width pointers
    // live separately: no assumption about 48-bit virtual addresses is required.
    // Group linear probing terminates even when every hash is identical.
    template <typename Meta>
    class TaggedDirectory {
        struct alignas(16) Tags {
            std::array<uint16_t, 8> values{};
            [[nodiscard]] unsigned matches(uint16_t tag) const noexcept {
                #if defined(_M_X64) || defined(__SSE2__)
                const auto data = _mm_load_si128(reinterpret_cast<const __m128i*>(values.data()));
                return static_cast<unsigned>(_mm_movemask_epi8(_mm_cmpeq_epi16(data, _mm_set1_epi16(std::bit_cast<int16_t>(tag)))));
                #else
                unsigned mask = 0;
                for (unsigned i = 0; i < 8; ++i) { if (values[i] == tag) { mask |= 3U << (2 * i); } }
                return mask;
                #endif
            }
        };
        using Pointers = std::array<Meta*, 8>;
        std::vector<Tags> tags_;
        std::vector<Pointers> pointers_;
        size_t size_{0};

        void insertPrepared(Meta* meta) noexcept {
            const size_t mask = tags_.size() - 1;
            size_t group = static_cast<size_t>(meta->key.hash) & mask;
            for (;;) {
                for (size_t i = 0; i < 8; ++i) {
                    if (tags_[group].values[i] == 0) {
                        pointers_[group][i] = meta;
                        tags_[group].values[i] = tagOf(meta->key.hash);
                        ++size_;
                        return;
                    }
                }
                group = (group + 1) & mask;
            }
        }

        public:
            struct Match { const Meta* meta; size_t length; uint64_t hash; };
            void reserve(size_t count) {
                if (count <= tags_.size() * 6) { return; }
                size_t groups = tags_.empty() ? 1 : tags_.size();
                while (count / 6 + static_cast<size_t>(count % 6 != 0) > groups) {
                    if (groups > std::numeric_limits<size_t>::max() / (2 * sizeof(Pointers))) {
                        throw std::length_error("Wormhole metadata directory too large");
                    }
                    groups *= 2;
                }
                TaggedDirectory replacement;
                replacement.tags_.resize(groups);
                replacement.pointers_.resize(groups);
                for (size_t group = 0; group < tags_.size(); ++group) {
                    for (size_t i = 0; i < 8; ++i) {
                        if (tags_[group].values[i] != 0) { replacement.insertPrepared(pointers_[group][i]); }
                    }
                }
                tags_.swap(replacement.tags_);
                pointers_.swap(replacement.pointers_);
            }
            // reserve() for the whole transaction must precede publication.
            void insert(Meta* meta) noexcept { insertPrepared(meta); }
            [[nodiscard]] Meta* find(const PrefixKey& key, bool exact = true) const noexcept {
                if (tags_.empty()) { return nullptr; }
                const size_t mask = tags_.size() - 1;
                size_t group = static_cast<size_t>(key.hash) & mask;
                const uint16_t tag = tagOf(key.hash);
                for (;;) {
                    unsigned matches = tags_[group].matches(tag);
                    while (matches != 0) {
                        const unsigned i = std::countr_zero(matches) / 2;
                        auto* meta = pointers_[group][i];
                        if (!exact || equalPrefix(meta->key, key)) { return meta; }
                        matches &= ~(3U << (2 * i));
                    }
                    if (tags_[group].matches(0) != 0) { return nullptr; }
                    group = (group + 1) & mask;
                }
            }
            [[nodiscard]] Match longestPrefix(std::span<const uint8_t> key, size_t limit, const Meta* root,
                HashRange extend = selectHashRange()) const noexcept {
                size_t low = 0, high = limit;
                uint64_t hash = HASH_SEED;
                const Meta* match = root;
                const auto search = [&](bool exact) {
                    while (low < high) {
                        const size_t middle = low + (high - low + 1) / 2;
                        const uint64_t candidate = extend(hash, key.data() + low, middle - low);
                        const auto* found = find(prefix(key, middle, candidate), exact);
                        if (!found) { high = middle - 1; }
                        else { low = middle; hash = candidate; match = found; }
                    }
                };
                // Section 3.1: tag-only probes cannot underestimate an LPM.
                // Validate the final prefix, and retry exactly on any collision.
                search(false);
                if (!equalPrefix(match->key, prefix(key, low, hash))) {
                    low = 0; high = limit; hash = HASH_SEED; match = root;
                    search(true);
                }
                return {match, low, hash};
            }
            [[nodiscard]] size_t size() const noexcept { return size_; }
            [[nodiscard]] size_t sizeBytes() const noexcept {
                return tags_.capacity() * sizeof(Tags) + pointers_.capacity() * sizeof(Pointers);
            }
    };
}

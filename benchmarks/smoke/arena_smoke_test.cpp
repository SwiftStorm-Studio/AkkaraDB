/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "TestErrorHandlers.hpp"
#include "akk/core/buffer/BufferArena.hpp"
#include "akk/core/utils/ArenaGenerator.hpp"

#include <array>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <stdexcept>

namespace {
    using akkaradb::core::ArenaGenerator;
    using akkaradb::core::BufferArena;

    void testBlockReuse() {
        BufferArena arena{64, 64};
        std::array<std::byte*, 8> blocks{};
        for (auto& block : blocks) { block = arena.allocate(64, 1); }
        for (int cycle = 0; cycle < 16; ++cycle) {
            arena.reset();
            for (auto* block : blocks) { AKK_TEST_CHECK(arena.allocate(64, 1) == block); }
        }

        arena.clear();
        AKK_TEST_CHECK(arena.allocate(1) != nullptr);
    }

    void testSkippedBlockReuse() {
        BufferArena arena{64, 256};
        (void)arena.allocate(64, 1);
        (void)arena.allocate(128, 1);
        auto* large = arena.allocate(256, 1);
        arena.reset();
        AKK_TEST_CHECK(arena.allocate(256, 1) == large);
    }

    void testAlignment() {
        BufferArena arena{8192, 8192};
        auto* base = arena.allocate(1, 1);
        for (size_t alignment = 1; alignment <= 1024; alignment *= 2) {
            auto* ptr = arena.allocate(3, alignment);
            AKK_TEST_CHECK(reinterpret_cast<uintptr_t>(ptr) % alignment == 0);
            AKK_TEST_CHECK(reinterpret_cast<uintptr_t>(ptr) >= reinterpret_cast<uintptr_t>(base));
            AKK_TEST_CHECK(reinterpret_cast<uintptr_t>(ptr) + 3 <= reinterpret_cast<uintptr_t>(base) + 8192);
            ptr[0] = std::byte{0x42};
        }
        arena.reset();
        auto* overAligned = arena.allocate(1024, 1024);
        AKK_TEST_CHECK(reinterpret_cast<uintptr_t>(overAligned) % 1024 == 0);

        BufferArena small{32, 32};
        (void)small.allocate(32, 1);
        auto* large = small.allocate(4096, 4096);
        AKK_TEST_CHECK(reinterpret_cast<uintptr_t>(large) % 4096 == 0);
        small.reset();
        AKK_TEST_CHECK(small.allocate(4096, 4096) == large);
    }

    void testInvalidRequests() {
        BufferArena arena{64, 64};
        AKK_TEST_CHECK(arena.allocate(0, 3) == nullptr);
        AKK_TEST_CHECK(arena.allocate(1, 0) != nullptr);
        bool rejected = false;
        try { (void)arena.allocate(1, 3); }
        catch (const std::invalid_argument&) { rejected = true; }
        AKK_TEST_CHECK(rejected);
        rejected = false;
        try { (void)arena.allocate(std::numeric_limits<size_t>::max(), 2); }
        catch (const std::bad_alloc&) { rejected = true; }
        AKK_TEST_CHECK(rejected);
        constexpr size_t hugeAlignment = size_t{1} << (std::numeric_limits<size_t>::digits - 1);
        rejected = false;
        try { (void)arena.allocate(hugeAlignment + 1, hugeAlignment); }
        catch (const std::bad_alloc&) { rejected = true; }
        AKK_TEST_CHECK(rejected);
        AKK_TEST_CHECK(arena.allocate(1) != nullptr);
    }

    struct alignas(__STDCPP_DEFAULT_NEW_ALIGNMENT__) AlignedValue {
        int value{};
    };

    ArenaGenerator<AlignedValue> alignedValues() {
        co_yield AlignedValue{42};
        co_yield AlignedValue{73};
    }

    void checkAlignedValues(ArenaGenerator<AlignedValue> generator) {
        int count = 0;
        for (const auto& value : generator) {
            AKK_TEST_CHECK(reinterpret_cast<uintptr_t>(&value) % alignof(AlignedValue) == 0);
            AKK_TEST_CHECK(value.value == (count == 0 ? 42 : 73));
            ++count;
        }
        AKK_TEST_CHECK(count == 2);
    }

    ArenaGenerator<int> throwingValues() {
        co_yield 7;
        throw std::runtime_error("generator failure");
    }

    void testGenerator() {
        BufferArena arena{8192, 8192};
        for (int cycle = 0; cycle < 8; ++cycle) {
            (void)arena.allocate(1, 1);
            checkAlignedValues(ArenaGenerator<AlignedValue>::withArena(arena, alignedValues));
            checkAlignedValues(alignedValues());
            checkAlignedValues(ArenaGenerator<AlignedValue>::withOwnedArena(64, 1024, alignedValues));
            arena.reset();
        }

        using Promise = ArenaGenerator<AlignedValue>::promise_type;
        bool rejected = false;
        try { (void)Promise::operator new(std::numeric_limits<size_t>::max()); }
        catch (const std::bad_alloc&) { rejected = true; }
        AKK_TEST_CHECK(rejected);

        rejected = false;
        try {
            auto generator = ArenaGenerator<int>::withArena(arena, throwingValues);
            auto it = generator.begin();
            AKK_TEST_CHECK(*it == 7);
            ++it;
        }
        catch (const std::runtime_error&) { rejected = true; }
        AKK_TEST_CHECK(rejected);

        // Allocation must use the heap again after the scoped arena is restored.
        auto heapGenerator = alignedValues();
        arena.clear();
        checkAlignedValues(std::move(heapGenerator));
    }
}

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();
    testBlockReuse();
    testSkippedBlockReuse();
    testAlignment();
    testInvalidRequests();
    testGenerator();
    std::puts("arena_smoke_test: OK");
}

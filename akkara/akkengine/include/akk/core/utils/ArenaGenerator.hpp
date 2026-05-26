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

// akkengine/include/akk/core/utils/ArenaGenerator.hpp
#pragma once

#include <coroutine>
#include <array>
#include <cstddef>
#include <exception>
#include <iterator>
#include <type_traits>
#include <utility>

#include "akk/core/buffer/BufferArena.hpp"

namespace akkaradb::core {
    /**
     * @brief Arena-backed coroutine generator.
     *
     * Coroutine frames are allocated from the currently active arena
     * (set via with_arena()).
     */
    template <typename T>
    class ArenaGenerator {
        public:
            struct promise_type;
            using handle_type = std::coroutine_handle<promise_type>;

            struct promise_type {
                T current_value_{};
                std::exception_ptr exception_{};

                #ifdef _MSC_VER
                #pragma warning(push)
                #pragma warning(disable: 4324)
                #endif

                struct alignas(std::max_align_t) AllocationHeader {
                    bool from_arena;
                };

                #ifdef _MSC_VER
                #pragma warning(pop)
                #endif

                static thread_local BufferArena* tls_arena_;

                [[nodiscard]] void* operator new(size_t size) {
                    const size_t total = size + sizeof(AllocationHeader);

                    if (tls_arena_ != nullptr) {
                        std::byte* raw = tls_arena_->allocate(total, alignof(std::max_align_t));
                        auto* header = reinterpret_cast<AllocationHeader*>(raw);
                        header->from_arena = true;
                        return raw + sizeof(AllocationHeader);
                    }

                    void* raw = ::operator new(total);
                    auto* header = static_cast<AllocationHeader*>(raw);
                    header->from_arena = false;
                    return reinterpret_cast<std::byte*>(raw) + sizeof(AllocationHeader);
                }

                static void operator delete(void* ptr, size_t) noexcept {
                    if (ptr == nullptr) { return; }

                    auto* header = reinterpret_cast<AllocationHeader*>(reinterpret_cast<std::byte*>(ptr) - sizeof(AllocationHeader));
                    if (!header->from_arena) { ::operator delete(header); }
                }

                static void operator delete(void* ptr) noexcept { operator delete(ptr, 0); }

                [[nodiscard]] ArenaGenerator get_return_object() noexcept { return ArenaGenerator{handle_type::from_promise(*this)}; }

                [[nodiscard]] std::suspend_always initial_suspend() noexcept { return {}; }
                [[nodiscard]] std::suspend_always final_suspend() noexcept { return {}; }
                void return_void() noexcept {}
                void unhandled_exception() noexcept { exception_ = std::current_exception(); }

                [[nodiscard]] std::suspend_always yield_value(T value) noexcept(std::is_nothrow_move_assignable_v<T>) {
                    current_value_ = std::move(value);
                    return {};
                }
            };

            class iterator {
                public:
                    using iterator_category = std::input_iterator_tag;
                    using value_type = T;
                    using difference_type = std::ptrdiff_t;
                    using pointer = const T*;
                    using reference = const T&;

                    iterator() noexcept = default;
                    explicit iterator(handle_type handle, bool done) noexcept : handle_{handle}, done_{done} {}

                    iterator& operator++() {
                        handle_.resume();
                        if (handle_.done()) {
                            if (handle_.promise().exception_) { std::rethrow_exception(handle_.promise().exception_); }
                            done_ = true;
                        }
                        return *this;
                    }

                    reference operator*() const noexcept { return handle_.promise().current_value_; }
                    pointer operator->() const noexcept { return &handle_.promise().current_value_; }

                    [[nodiscard]] bool operator==(std::default_sentinel_t) const noexcept { return done_; }

                private:
                    handle_type handle_{};
                    bool done_{true};
            };

            ArenaGenerator() noexcept = default;

            explicit ArenaGenerator(handle_type handle) noexcept : handle_{handle} {}

            ArenaGenerator(const ArenaGenerator&) = delete;
            ArenaGenerator& operator=(const ArenaGenerator&) = delete;

            ArenaGenerator(ArenaGenerator&& other) noexcept : handle_{std::exchange(other.handle_, {})} {}

            ArenaGenerator& operator=(ArenaGenerator&& other) noexcept {
                if (this != &other) {
                    if (handle_) { handle_.destroy(); }
                    handle_ = std::exchange(other.handle_, {});
                }
                return *this;
            }

            ~ArenaGenerator() { if (handle_) { handle_.destroy(); } }

            [[nodiscard]] iterator begin() {
                if (!handle_) { return iterator{}; }

                handle_.resume();
                if (handle_.done()) {
                    if (handle_.promise().exception_) { std::rethrow_exception(handle_.promise().exception_); }
                    return iterator{handle_, true};
                }

                return iterator{handle_, false};
            }

            [[nodiscard]] std::default_sentinel_t end() const noexcept { return {}; }

            template <typename Factory>
            [[nodiscard]] static ArenaGenerator with_arena(BufferArena& arena, Factory&& factory) {
                struct ScopedArena {
                    explicit ScopedArena(BufferArena* arena_ptr) noexcept : prev_{promise_type::tls_arena_} { promise_type::tls_arena_ = arena_ptr; }
                    ~ScopedArena() { promise_type::tls_arena_ = prev_; }
                    BufferArena* prev_;
                };

                ScopedArena scoped{&arena};
                return std::forward<Factory>(factory)();
            }

            [[nodiscard]] static ArenaGenerator yield_all(BufferArena& arena, ArenaGenerator first, ArenaGenerator second) {
                return yieldAll(arena, std::move(first), std::move(second));
            }

            [[nodiscard]] static ArenaGenerator yieldAll(BufferArena& arena, ArenaGenerator first, ArenaGenerator second) {
                return with_arena(
                    arena,
                    [first = std::move(first), second = std::move(second)]() mutable { return yield_all_impl(std::move(first), std::move(second)); }
                );
            }

            [[nodiscard]] static ArenaGenerator yieldAll(BufferArena& arena, ArenaGenerator first) {
                return with_arena(arena, [first = std::move(first)]() mutable { return yield_all_impl(std::move(first), ArenaGenerator{}); });
            }

            template <typename... Rest>
            [[nodiscard]] static ArenaGenerator yieldAll(BufferArena& arena, ArenaGenerator first, ArenaGenerator second, Rest... rest) {
                static_assert((std::is_same_v<ArenaGenerator, std::remove_cvref_t<Rest>> && ...), "yieldAll rest arguments must be ArenaGenerator<T>");

                return with_arena(
                    arena,
                    [first = std::move(first), second = std::move(second), tail = std::array<ArenaGenerator, sizeof...(Rest)>{std::move(rest)...}]() mutable {
                        return yield_all_many_impl(std::move(first), std::move(second), std::move(tail));
                    }
                );
            }

        private:
            [[nodiscard]] static ArenaGenerator yield_all_impl(ArenaGenerator first, ArenaGenerator second) {
                for (auto&& value : first) { co_yield value; }
                for (auto&& value : second) { co_yield value; }
            }

            template <size_t N>
            [[nodiscard]] static ArenaGenerator yield_all_many_impl(ArenaGenerator first, ArenaGenerator second, std::array<ArenaGenerator, N> tail) {
                for (auto&& value : first) { co_yield value; }
                for (auto&& value : second) { co_yield value; }
                for (auto& gen : tail) { for (auto&& value : gen) { co_yield value; } }
            }

            handle_type handle_{};
    };

    template <typename T>
    thread_local BufferArena* ArenaGenerator<T>::promise_type::tls_arena_ = nullptr;
} // namespace akkaradb::core

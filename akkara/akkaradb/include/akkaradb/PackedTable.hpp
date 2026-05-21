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

// akkaradb/include/akkaradb/PackedTable.hpp
#pragma once

#include "binpack/BinPack.hpp"
#include "binpack/detail/MemberPtrTraits.hpp"
#include "detail/Hash.hpp"
#include "engine/AkkEngine.hpp"
#include "core/buffer/BufferArena.hpp"
#include "core/record/KeyFingerprint.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

namespace akkaradb {
    namespace query {
        template <typename Entity>
        struct ProxyTag {};

        enum class Op {
            Eq, Ne, Gt, Ge, Lt, Le, And, Or, In, NotIn, StartsWith, Contains, Like
        };

        struct AlwaysTrue {};

        template <Op Operator, typename L, typename R>
        struct Compare;

        template <auto FieldPtr>
        struct Column {
            static constexpr auto field_ptr = FieldPtr;

            template <typename R>
            [[nodiscard]] auto in(R&& rhs) const;

            template <typename T>
            [[nodiscard]] auto in(std::initializer_list<T> rhs) const;

            template <typename R>
            [[nodiscard]] auto not_in(R&& rhs) const;

            template <typename T>
            [[nodiscard]] auto not_in(std::initializer_list<T> rhs) const;

            template <typename R>
            [[nodiscard]] auto starts_with(R&& rhs) const;

            template <typename R>
            [[nodiscard]] auto contains(R&& rhs) const;

            template <typename R>
            [[nodiscard]] auto like(R&& rhs) const;
        };

        template <typename T>
        struct Literal {
            T value;
        };

        template <Op Operator, typename L, typename R>
        struct Compare {
            static constexpr Op op = Operator;
            L lhs;
            R rhs;
        };

        template <Op Operator, typename L, typename R>
        struct Logical {
            static constexpr Op op = Operator;
            L lhs;
            R rhs;
        };

        template <typename X>
        struct Not {
            X x;
        };

        template <typename T>
        struct is_expr : std::false_type {};

        template <>
        struct is_expr<AlwaysTrue> : std::true_type {};

        template <auto FieldPtr>
        struct is_expr<Column<FieldPtr>> : std::true_type {};

        template <typename T>
        struct is_expr<Literal<T>> : std::true_type {};

        template <Op Operator, typename L, typename R>
        struct is_expr<Compare<Operator, L, R>> : std::true_type {};

        template <Op Operator, typename L, typename R>
        struct is_expr<Logical<Operator, L, R>> : std::true_type {};

        template <typename X>
        struct is_expr<Not<X>> : std::true_type {};

        template <typename T>
        inline constexpr bool is_expr_v = is_expr<std::remove_cvref_t<T>>::value;

        template <typename T>
        using literal_storage_t = std::conditional_t<std::is_convertible_v<T, std::string_view> && !std::is_arithmetic_v<std::remove_cvref_t<T>>, std::string,
                                                     std::remove_cvref_t<T>>;

        template <typename T>
        [[nodiscard]] auto normalize_literal(T&& value) {
            if constexpr (std::is_convertible_v<T, std::string_view> && !std::is_arithmetic_v<std::remove_cvref_t<T>>) {
                return std::string{std::string_view{value}};
            }
            else { return std::forward<T>(value); }
        }

        template <typename T>
        [[nodiscard]] auto as_expr(T&& value) {
            if constexpr (is_expr_v<T>) { return std::forward<T>(value); }
            else { return Literal<literal_storage_t<T>>{normalize_literal(std::forward<T>(value))}; }
        }

        template <typename L, typename R> requires(is_expr_v<L> || is_expr_v<R>)
        [[nodiscard]] auto operator==(L&& lhs, R&& rhs) {
            return Compare<Op::Eq, decltype(as_expr(std::forward<L>(lhs))), decltype(as_expr(std::forward<R>(rhs)))>{
                as_expr(std::forward<L>(lhs)),
                as_expr(std::forward<R>(rhs))
            };
        }

        template <typename L, typename R> requires(is_expr_v<L> || is_expr_v<R>)
        [[nodiscard]] auto operator!=(L&& lhs, R&& rhs) {
            return Compare<Op::Ne, decltype(as_expr(std::forward<L>(lhs))), decltype(as_expr(std::forward<R>(rhs)))>{
                as_expr(std::forward<L>(lhs)),
                as_expr(std::forward<R>(rhs))
            };
        }

        template <typename L, typename R> requires(is_expr_v<L> || is_expr_v<R>)
        [[nodiscard]] auto operator>(L&& lhs, R&& rhs) {
            return Compare<Op::Gt, decltype(as_expr(std::forward<L>(lhs))), decltype(as_expr(std::forward<R>(rhs)))>{
                as_expr(std::forward<L>(lhs)),
                as_expr(std::forward<R>(rhs))
            };
        }

        template <typename L, typename R> requires(is_expr_v<L> || is_expr_v<R>)
        [[nodiscard]] auto operator>=(L&& lhs, R&& rhs) {
            return Compare<Op::Ge, decltype(as_expr(std::forward<L>(lhs))), decltype(as_expr(std::forward<R>(rhs)))>{
                as_expr(std::forward<L>(lhs)),
                as_expr(std::forward<R>(rhs))
            };
        }

        template <typename L, typename R> requires(is_expr_v<L> || is_expr_v<R>)
        [[nodiscard]] auto operator<(L&& lhs, R&& rhs) {
            return Compare<Op::Lt, decltype(as_expr(std::forward<L>(lhs))), decltype(as_expr(std::forward<R>(rhs)))>{
                as_expr(std::forward<L>(lhs)),
                as_expr(std::forward<R>(rhs))
            };
        }

        template <typename L, typename R> requires(is_expr_v<L> || is_expr_v<R>)
        [[nodiscard]] auto operator<=(L&& lhs, R&& rhs) {
            return Compare<Op::Le, decltype(as_expr(std::forward<L>(lhs))), decltype(as_expr(std::forward<R>(rhs)))>{
                as_expr(std::forward<L>(lhs)),
                as_expr(std::forward<R>(rhs))
            };
        }

        template <typename L, typename R> requires(is_expr_v<L> && is_expr_v<R>)
        [[nodiscard]] auto operator&&(L&& lhs, R&& rhs) {
            return Logical<Op::And, std::remove_cvref_t<L>, std::remove_cvref_t<R>>{std::forward<L>(lhs), std::forward<R>(rhs)};
        }

        template <typename L, typename R> requires(is_expr_v<L> && is_expr_v<R>)
        [[nodiscard]] auto operator||(L&& lhs, R&& rhs) {
            return Logical<Op::Or, std::remove_cvref_t<L>, std::remove_cvref_t<R>>{std::forward<L>(lhs), std::forward<R>(rhs)};
        }

        template <typename X> requires(is_expr_v<X>)
        [[nodiscard]] auto operator!(X&& x) { return Not<std::remove_cvref_t<X>>{std::forward<X>(x)}; }

        template <typename L, typename R> requires(is_expr_v<L> || is_expr_v<R>)
        [[nodiscard]] auto in(L&& lhs, R&& rhs) {
            return Compare<Op::In, decltype(as_expr(std::forward<L>(lhs))), decltype(as_expr(std::forward<R>(rhs)))>{
                as_expr(std::forward<L>(lhs)),
                as_expr(std::forward<R>(rhs))
            };
        }

        template <typename L, typename T> requires(is_expr_v<L>)
        [[nodiscard]] auto in(L&& lhs, std::initializer_list<T> rhs) {
            return in(std::forward<L>(lhs), std::vector<literal_storage_t<T>>{rhs.begin(), rhs.end()});
        }

        template <typename L, typename R> requires(is_expr_v<L> || is_expr_v<R>)
        [[nodiscard]] auto not_in(L&& lhs, R&& rhs) {
            return Compare<Op::NotIn, decltype(as_expr(std::forward<L>(lhs))), decltype(as_expr(std::forward<R>(rhs)))>{
                as_expr(std::forward<L>(lhs)),
                as_expr(std::forward<R>(rhs))
            };
        }

        template <typename L, typename T> requires(is_expr_v<L>)
        [[nodiscard]] auto not_in(L&& lhs, std::initializer_list<T> rhs) {
            return not_in(std::forward<L>(lhs), std::vector<literal_storage_t<T>>{rhs.begin(), rhs.end()});
        }

        template <typename L, typename R> requires(is_expr_v<L> || is_expr_v<R>)
        [[nodiscard]] auto starts_with(L&& lhs, R&& rhs) {
            return Compare<Op::StartsWith, decltype(as_expr(std::forward<L>(lhs))), decltype(as_expr(std::forward<R>(rhs)))>{
                as_expr(std::forward<L>(lhs)),
                as_expr(std::forward<R>(rhs))
            };
        }

        template <typename L, typename R> requires(is_expr_v<L> || is_expr_v<R>)
        [[nodiscard]] auto contains(L&& lhs, R&& rhs) {
            return Compare<Op::Contains, decltype(as_expr(std::forward<L>(lhs))), decltype(as_expr(std::forward<R>(rhs)))>{
                as_expr(std::forward<L>(lhs)),
                as_expr(std::forward<R>(rhs))
            };
        }

        template <typename L, typename R> requires(is_expr_v<L> || is_expr_v<R>)
        [[nodiscard]] auto like(L&& lhs, R&& rhs) {
            return Compare<Op::Like, decltype(as_expr(std::forward<L>(lhs))), decltype(as_expr(std::forward<R>(rhs)))>{
                as_expr(std::forward<L>(lhs)),
                as_expr(std::forward<R>(rhs))
            };
        }

        template <auto FieldPtr>
        template <typename R>
        [[nodiscard]] auto Column<FieldPtr>::in(R&& rhs) const {
            return query::in(*this, std::forward<R>(rhs));
        }

        template <auto FieldPtr>
        template <typename T>
        [[nodiscard]] auto Column<FieldPtr>::in(std::initializer_list<T> rhs) const {
            return query::in(*this, rhs);
        }

        template <auto FieldPtr>
        template <typename R>
        [[nodiscard]] auto Column<FieldPtr>::not_in(R&& rhs) const {
            return query::not_in(*this, std::forward<R>(rhs));
        }

        template <auto FieldPtr>
        template <typename T>
        [[nodiscard]] auto Column<FieldPtr>::not_in(std::initializer_list<T> rhs) const {
            return query::not_in(*this, rhs);
        }

        template <auto FieldPtr>
        template <typename R>
        [[nodiscard]] auto Column<FieldPtr>::starts_with(R&& rhs) const {
            return query::starts_with(*this, std::forward<R>(rhs));
        }

        template <auto FieldPtr>
        template <typename R>
        [[nodiscard]] auto Column<FieldPtr>::contains(R&& rhs) const {
            return query::contains(*this, std::forward<R>(rhs));
        }

        template <auto FieldPtr>
        template <typename R>
        [[nodiscard]] auto Column<FieldPtr>::like(R&& rhs) const {
            return query::like(*this, std::forward<R>(rhs));
        }

        template <typename Entity>
        [[nodiscard]] auto make_proxy() { return akkaradb_query_proxy(ProxyTag<Entity>{}); }

        template <typename Entity>
        using QueryProxy = decltype(make_proxy<Entity>());

        template <typename T>
        struct is_column : std::false_type {};

        template <auto FieldPtr>
        struct is_column<Column<FieldPtr>> : std::true_type {};

        template <typename T>
        inline constexpr bool is_column_v = is_column<std::remove_cvref_t<T>>::value;

        template <typename T>
        struct is_literal : std::false_type {};

        template <typename T>
        struct is_literal<Literal<T>> : std::true_type {};

        template <typename T>
        inline constexpr bool is_literal_v = is_literal<std::remove_cvref_t<T>>::value;

        template <typename T>
        struct is_compare : std::false_type {};

        template <Op Operator, typename L, typename R>
        struct is_compare<Compare<Operator, L, R>> : std::true_type {};

        template <typename T>
        inline constexpr bool is_compare_v = is_compare<std::remove_cvref_t<T>>::value;

        template <typename T>
        struct is_and : std::false_type {};

        template <typename L, typename R>
        struct is_and<Logical<Op::And, L, R>> : std::true_type {};

        template <typename T>
        inline constexpr bool is_and_v = is_and<std::remove_cvref_t<T>>::value;

        template <Op Operator>
        inline constexpr Op swapped_compare_op = Operator;

        template <>
        inline constexpr Op swapped_compare_op<Op::Gt> = Op::Lt;

        template <>
        inline constexpr Op swapped_compare_op<Op::Ge> = Op::Le;

        template <>
        inline constexpr Op swapped_compare_op<Op::Lt> = Op::Gt;

        template <>
        inline constexpr Op swapped_compare_op<Op::Le> = Op::Ge;

        template <typename T>
        [[nodiscard]] const auto& literal_value(const Literal<T>& literal) noexcept { return literal.value; }

        template <typename Entity>
        [[nodiscard]] bool eval(const AlwaysTrue&, const Entity&) { return true; }

        template <auto FieldPtr, typename Entity>
        [[nodiscard]] auto eval(const Column<FieldPtr>&, const Entity& entity) { return entity.*FieldPtr; }

        template <typename T, typename Entity>
        [[nodiscard]] const T& eval(const Literal<T>& literal, const Entity&) { return literal.value; }

        template <typename Needle, typename Haystack>
        [[nodiscard]] bool contains_value(const Haystack& haystack, const Needle& needle) {
            for (const auto& value : haystack) {
                if (value == needle) { return true; }
            }
            return false;
        }

        template <typename Value, typename Pattern>
        [[nodiscard]] bool string_starts_with(const Value& value, const Pattern& pattern) {
            std::string_view haystack{value};
            std::string_view needle{pattern};
            return haystack.starts_with(needle);
        }

        template <typename Value, typename Pattern>
        [[nodiscard]] bool string_contains(const Value& value, const Pattern& pattern) {
            std::string_view haystack{value};
            std::string_view needle{pattern};
            return haystack.find(needle) != std::string_view::npos;
        }

        [[nodiscard]] inline bool like_match(std::string_view value, size_t vi, std::string_view pattern, size_t pi) {
            while (pi < pattern.size()) {
                if (pattern[pi] == '%') {
                    while (pi + 1 < pattern.size() && pattern[pi + 1] == '%') { ++pi; }
                    if (pi + 1 == pattern.size()) { return true; }
                    for (size_t next = vi; next <= value.size(); ++next) {
                        if (like_match(value, next, pattern, pi + 1)) { return true; }
                    }
                    return false;
                }
                if (pattern[pi] == '_') {
                    if (vi >= value.size()) { return false; }
                    ++vi;
                    ++pi;
                    continue;
                }
                if (vi >= value.size() || value[vi] != pattern[pi]) { return false; }
                ++vi;
                ++pi;
            }
            return vi == value.size();
        }

        template <typename Value, typename Pattern>
        [[nodiscard]] bool string_like(const Value& value, const Pattern& pattern) {
            return like_match(std::string_view{value}, 0, std::string_view{pattern}, 0);
        }

        template <Op Operator, typename L, typename R, typename Entity>
        [[nodiscard]] bool eval(const Compare<Operator, L, R>& expr, const Entity& entity) {
            const auto lhs = eval(expr.lhs, entity);
            const auto rhs = eval(expr.rhs, entity);
            if constexpr (Operator == Op::Eq) { return lhs == rhs; }
            else if constexpr (Operator == Op::Ne) { return lhs != rhs; }
            else if constexpr (Operator == Op::Gt) { return lhs > rhs; }
            else if constexpr (Operator == Op::Ge) { return lhs >= rhs; }
            else if constexpr (Operator == Op::Lt) { return lhs < rhs; }
            else if constexpr (Operator == Op::Le) { return lhs <= rhs; }
            else if constexpr (Operator == Op::In) { return contains_value(rhs, lhs); }
            else if constexpr (Operator == Op::NotIn) { return !contains_value(rhs, lhs); }
            else if constexpr (Operator == Op::StartsWith) { return string_starts_with(lhs, rhs); }
            else if constexpr (Operator == Op::Contains) { return string_contains(lhs, rhs); }
            else if constexpr (Operator == Op::Like) { return string_like(lhs, rhs); }
        }

        template <Op Operator, typename L, typename R, typename Entity>
        [[nodiscard]] bool eval(const Logical<Operator, L, R>& expr, const Entity& entity) {
            if constexpr (Operator == Op::And) { return eval(expr.lhs, entity) && eval(expr.rhs, entity); }
            else { return eval(expr.lhs, entity) || eval(expr.rhs, entity); }
        }

        template <typename X, typename Entity>
        [[nodiscard]] bool eval(const Not<X>& expr, const Entity& entity) { return !eval(expr.x, entity); }
    } // namespace query

    #define AKKARADB_QUERYABLE_FIELD(Type, Field) ::akkaradb::query::Column<&Type::Field> Field{};
    #define AKKARADB_QUERYABLE(Type, A, B, C, D) \
        [[nodiscard]] inline auto akkaradb_query_proxy(::akkaradb::query::ProxyTag<Type>) { \
            struct Proxy { \
                AKKARADB_QUERYABLE_FIELD(Type, A) \
                AKKARADB_QUERYABLE_FIELD(Type, B) \
                AKKARADB_QUERYABLE_FIELD(Type, C) \
                AKKARADB_QUERYABLE_FIELD(Type, D) \
            }; \
            return Proxy{}; \
        }

    template <auto PrimaryKeyPtr>
    class PackedTable {
        class ArenaByteBuffer {
            public:
                ArenaByteBuffer() = default;
                explicit ArenaByteBuffer(core::BufferArena* arena) noexcept : arena_{arena} {}
                ArenaByteBuffer(const ArenaByteBuffer&) = default;
                ArenaByteBuffer(ArenaByteBuffer&&) noexcept = default;
                ArenaByteBuffer& operator=(ArenaByteBuffer&&) noexcept = default;

                ArenaByteBuffer& operator=(const ArenaByteBuffer& other) {
                    if (this != &other) {
                        clear();
                        insert(end(), other.begin(), other.end());
                    }
                    return *this;
                }

                void bind(core::BufferArena* arena) noexcept {
                    arena_ = arena;
                    release();
                }

                void release() noexcept {
                    data_ = nullptr;
                    size_ = 0;
                    capacity_ = 0;
                }

                void clear() noexcept { size_ = 0; }

                void reserve(size_t capacity) {
                    if (capacity <= capacity_) { return; }
                    ensure_arena();
                    std::byte* raw = arena_->allocate(capacity, alignof(uint8_t));
                    auto* next = reinterpret_cast<uint8_t*>(raw);
                    if (data_ != nullptr && size_ != 0) { std::memcpy(next, data_, size_); }
                    data_ = next;
                    capacity_ = capacity;
                }

                void resize(size_t size) {
                    const size_t old_size = size_;
                    reserve(size);
                    if (size > old_size) { std::memset(data_ + old_size, 0, size - old_size); }
                    size_ = size;
                }

                template <typename It>
                void assign(It first, It last) {
                    clear();
                    insert(end(), first, last);
                }

                template <typename It>
                void insert(uint8_t*, It first, It last) {
                    const size_t add = static_cast<size_t>(std::distance(first, last));
                    reserve(size_ + add);
                    for (; first != last; ++first) { data_[size_++] = static_cast<uint8_t>(*first); }
                }

                void push_back(uint8_t value) {
                    reserve(size_ + 1);
                    data_[size_++] = value;
                }

                [[nodiscard]] uint8_t* data() noexcept { return data_; }
                [[nodiscard]] const uint8_t* data() const noexcept { return data_; }
                [[nodiscard]] size_t size() const noexcept { return size_; }
                [[nodiscard]] bool empty() const noexcept { return size_ == 0; }
                [[nodiscard]] uint8_t* begin() noexcept { return data_; }
                [[nodiscard]] uint8_t* end() noexcept { return data_ == nullptr ? nullptr : data_ + size_; }
                [[nodiscard]] const uint8_t* begin() const noexcept { return data_; }
                [[nodiscard]] const uint8_t* end() const noexcept { return data_ == nullptr ? nullptr : data_ + size_; }
                [[nodiscard]] uint8_t& operator[](size_t index) noexcept { return data_[index]; }
                [[nodiscard]] const uint8_t& operator[](size_t index) const noexcept { return data_[index]; }
                [[nodiscard]] operator std::span<const uint8_t>() const noexcept { return {data_, size_}; }

            private:
                void ensure_arena() const { if (arena_ == nullptr) { throw std::logic_error("PackedTable: arena buffer is not bound"); } }

                core::BufferArena* arena_ = nullptr;
                uint8_t* data_ = nullptr;
                size_t size_ = 0;
                size_t capacity_ = 0;
        };

        public:
            using Entity = binpack::detail::class_of<PrimaryKeyPtr>;
            using PK = binpack::detail::member_of<PrimaryKeyPtr>;

            struct Entry {
                PK id;
                Entity value;
            };

            PackedTable(PackedTable&&) noexcept = default;
            PackedTable& operator=(PackedTable&&) noexcept = default;
            PackedTable(const PackedTable&) = delete;
            PackedTable& operator=(const PackedTable&) = delete;

            template <auto FieldPtr>
            class Index;

            template <auto FieldPtr>
            [[nodiscard]] Index<FieldPtr> index() {
                static_assert(std::is_same_v<binpack::detail::class_of<FieldPtr>, Entity>, "index field must belong to the table entity");
                const std::string_view field_name = binpack::detail::member_name<FieldPtr>();
                const auto prefix = make_index_prefix(table_name_, field_name);

                for (const auto& idx : indexes_) { if (idx.field_name == field_name) { return Index<FieldPtr>{this, prefix}; } }

                indexes_.push_back(
                    IndexDef{
                        prefix,
                        std::string(field_name),
                        [](const Entity& entity, ArenaByteBuffer& out) {
                            out.clear();
                            encode_index_field_value(entity.*FieldPtr, out);
                        }
                    }
                );
                return Index<FieldPtr>{this, prefix};
            }

            template <auto FieldPtr>
            PackedTable& indexed() {
                (void)index<FieldPtr>();
                return *this;
            }

            void put(const Entity& entity) {
                reset_temp_buffers();
                const PK& pk = entity.*PrimaryKeyPtr;
                make_pk_key(pk, pk_key_buffer_);

                if (!indexes_.empty()) {
                    std::span<const uint8_t> old_bytes;
                    if (engine_->get_into_arena(pk_key_buffer_, *temp_arena_, old_bytes)) {
                        const Entity old_entity = binpack::BinPack::decode<Entity>(old_bytes);
                        remove_index_entries(old_entity, pk_key_buffer_);
                    }
                }

                value_buffer_.clear();
                value_buffer_.reserve(binpack::BinPack::estimate_size(entity));
                binpack::BinPack::encode_into(entity, value_buffer_);

                put_hinted(pk_key_buffer_, value_buffer_);
                if (!indexes_.empty()) { write_index_entries(entity, pk_key_buffer_); }
            }

            [[nodiscard]] std::optional<Entity> get(const PK& pk) const {
                Entity out{};
                if (!get_into(pk, out)) { return std::nullopt; }
                return out;
            }

            [[nodiscard]] bool get_into(const PK& pk, Entity& out) const {
                reset_temp_buffers();
                make_pk_key(pk, pk_key_buffer_);
                std::span<const uint8_t> bytes;
                if (!engine_->get_into_arena(pk_key_buffer_, *temp_arena_, bytes)) { return false; }
                return binpack::BinPack::decode_into<Entity>(bytes, out);
            }

            void remove(const PK& pk) {
                reset_temp_buffers();
                make_pk_key(pk, pk_key_buffer_);

                if (!indexes_.empty()) {
                    std::span<const uint8_t> old_bytes;
                    if (engine_->get_into_arena(pk_key_buffer_, *temp_arena_, old_bytes)) {
                        const Entity old_entity = binpack::BinPack::decode<Entity>(old_bytes);
                        remove_index_entries(old_entity, pk_key_buffer_);
                    }
                }

                remove_hinted(pk_key_buffer_);
            }

            [[nodiscard]] bool exists(const PK& pk) const {
                reset_temp_buffers();
                make_pk_key(pk, pk_key_buffer_);
                return engine_->exists(pk_key_buffer_);
            }

            void upsert(const PK& pk, std::function<void(Entity&)> update) {
                Entity entity = get(pk).value_or(Entity{});
                entity.*PrimaryKeyPtr = pk;
                update(entity);
                put(entity);
            }

            template <auto FieldPtr>
            [[nodiscard]] std::optional<Entity> find_by(const binpack::detail::member_of<FieldPtr>& value) const {
                static_assert(std::is_same_v<binpack::detail::class_of<FieldPtr>, Entity>, "find_by field must belong to the table entity");
                const std::string_view field_name = binpack::detail::member_name<FieldPtr>();
                const auto prefix = make_index_prefix(table_name_, field_name);

                bool registered = false;
                for (const auto& idx : indexes_) {
                    if (idx.field_name == field_name) {
                        registered = true;
                        break;
                    }
                }
                if (!registered) { throw std::runtime_error("PackedTable::find_by: index is not registered for this field"); }

                reset_temp_buffers();
                field_buffer_.clear();
                encode_index_field_value(value, field_buffer_);
                make_index_search_prefix(prefix, field_buffer_, scan_start_buffer_);
                scan_end_buffer_ = scan_start_buffer_;
                if (!detail::increment_be_bytes(scan_end_buffer_.data(), scan_end_buffer_.size())) { scan_end_buffer_.clear(); }

                core::BufferArena scan_arena;
                auto rows = engine_->scan(scan_arena, scan_start_buffer_, scan_end_buffer_);
                for (auto it = rows.begin(); !(it == rows.end()); ++it) {
                    const auto& raw = *it;
                    const auto key = raw.key;
                    if (key.size() <= scan_start_buffer_.size()) { continue; }

                    Entry entry;
                    const std::span<const uint8_t> pk_bytes{key.data() + scan_start_buffer_.size(), key.size() - scan_start_buffer_.size()};
                    if (get_by_pk_bytes(pk_bytes, entry)) { return entry.value; }
                }
                return std::nullopt;
            }

            [[nodiscard]] size_t count() const {
                reset_temp_buffers();
                make_prefix_start_end(pk_prefix_, scan_start_buffer_, scan_end_buffer_);
                return engine_->count(scan_start_buffer_, scan_end_buffer_);
            }

            class ScanRange {
                public:
                    ScanRange(ScanRange&&) noexcept = default;
                    ScanRange& operator=(ScanRange&&) noexcept = default;
                    ScanRange(const ScanRange&) = delete;
                    ScanRange& operator=(const ScanRange&) = delete;

                    [[nodiscard]] bool has_next() const noexcept { return pending_.has_value(); }

                    [[nodiscard]] Entry next() {
                        if (!pending_) { throw std::out_of_range("PackedTable::ScanRange: no next entry"); }
                        Entry out = std::move(*pending_);
                        advance();
                        return out;
                    }

                private:
                    friend class PackedTable;

                    ScanRange(const PackedTable* table, std::span<const uint8_t> start_key, std::span<const uint8_t> end_key)
                        : table_{table},
                          scan_arena_{std::make_unique<core::BufferArena>()},
                          rows_{table_->engine_->scan(*scan_arena_, start_key, end_key)},
                          it_{rows_.begin()} { advance(); }

                    void advance() {
                        pending_.reset();
                        while (!(it_ == rows_.end())) {
                            const auto& raw = *it_;
                            const auto key = raw.key;
                            if (key.size() < table_->pk_prefix_.size() || std::memcmp(key.data(), table_->pk_prefix_.data(), table_->pk_prefix_.size()) != 0) {
                                return;
                            }

                            std::span<const uint8_t> pk_bytes{key.data() + table_->pk_prefix_.size(), key.size() - table_->pk_prefix_.size()};
                            pending_ = Entry{binpack::BinPack::decode<PK>(pk_bytes), binpack::BinPack::decode<Entity>(raw.value)};
                            ++it_;
                            return;
                        }
                    }

                    const PackedTable* table_;
                    std::unique_ptr<core::BufferArena> scan_arena_;
                    core::ArenaGenerator<engine::AkkEngine::ScanRecordView> rows_;
                    core::ArenaGenerator<engine::AkkEngine::ScanRecordView>::iterator it_;
                    std::optional<Entry> pending_;
            };

            [[nodiscard]] ScanRange scan_all() const {
                reset_temp_buffers();
                make_prefix_start_end(pk_prefix_, scan_start_buffer_, scan_end_buffer_);
                return ScanRange{this, scan_start_buffer_, scan_end_buffer_};
            }

            [[nodiscard]] ScanRange scan(const PK& start_pk) const {
                reset_temp_buffers();
                make_pk_key(start_pk, scan_start_buffer_);
                scan_end_buffer_.assign(pk_prefix_.begin(), pk_prefix_.end());
                if (!detail::increment_be_bytes(scan_end_buffer_.data(), scan_end_buffer_.size())) { scan_end_buffer_.clear(); }
                return ScanRange{this, scan_start_buffer_, scan_end_buffer_};
            }

            [[nodiscard]] ScanRange scan(const PK& start_pk, const PK& end_pk) const {
                reset_temp_buffers();
                make_pk_key(start_pk, scan_start_buffer_);
                make_pk_key(end_pk, scan_end_buffer_);
                return ScanRange{this, scan_start_buffer_, scan_end_buffer_};
            }

            enum class QuerySourceKind {
                Table, Index
            };

            struct QueryRange {
                std::vector<uint8_t> start_key;
                std::vector<uint8_t> end_key;
                size_t index_search_prefix_size = 0;
                bool dynamic_index_pk_offset = false;
                bool dedupe_index_pks = false;
            };

            struct QueryPlan {
                QuerySourceKind kind = QuerySourceKind::Table;
                std::vector<QueryRange> ranges;
            };

            class QuerySource {
                public:
                    QuerySource(QuerySource&&) noexcept = default;
                    QuerySource& operator=(QuerySource&&) noexcept = default;
                    QuerySource(const QuerySource&) = delete;
                    QuerySource& operator=(const QuerySource&) = delete;

                    [[nodiscard]] bool has_next() const noexcept { return pending_.has_value(); }

                    [[nodiscard]] Entry next() {
                        if (!pending_) { throw std::out_of_range("PackedTable::QuerySource: no next entry"); }
                        Entry out = std::move(*pending_);
                        advance();
                        return out;
                    }

                private:
                    friend class PackedTable;

                    QuerySource(const PackedTable* table, QueryPlan plan)
                        : table_{table},
                          kind_{plan.kind},
                          ranges_{std::move(plan.ranges)},
                          scan_arena_{std::make_unique<core::BufferArena>()} {
                        open_next_range();
                        advance();
                    }

                    void open_next_range() {
                        while (range_index_ < ranges_.size()) {
                            const auto& range = ranges_[range_index_++];
                            index_search_prefix_size_ = range.index_search_prefix_size;
                            dynamic_index_pk_offset_ = range.dynamic_index_pk_offset;
                            dedupe_index_pks_ = range.dedupe_index_pks;
                            it_ = {};
                            rows_ = {};
                            scan_arena_->reset();
                            rows_ = table_->engine_->scan(*scan_arena_, range.start_key, range.end_key);
                            it_ = rows_.begin();
                            return;
                        }
                    }

                    void advance() {
                        pending_.reset();
                        while (range_index_ <= ranges_.size()) {
                            while (!(it_ == rows_.end())) {
                                const auto& raw = *it_;
                                ++it_;
                                Entry entry;
                                if (kind_ == QuerySourceKind::Table) {
                                    const auto key = raw.key;
                                    if (key.size() < table_->pk_prefix_.size() || std::memcmp(
                                        key.data(),
                                        table_->pk_prefix_.data(),
                                        table_->pk_prefix_.size()
                                    ) != 0) { return; }

                                    std::span<const uint8_t> pk_bytes{key.data() + table_->pk_prefix_.size(), key.size() - table_->pk_prefix_.size()};
                                    entry = Entry{binpack::BinPack::decode<PK>(pk_bytes), binpack::BinPack::decode<Entity>(raw.value)};
                                }
                                else {
                                    const auto key = raw.key;
                                    size_t pk_offset = index_search_prefix_size_;
                                    if (dynamic_index_pk_offset_) {
                                        if (key.size() <= 12) { continue; }
                                        const size_t field_size = read_be32(key.data() + 8);
                                        pk_offset = 12 + field_size;
                                    }
                                    if (key.size() <= pk_offset) { continue; }
                                    const std::span<const uint8_t> pk_bytes{key.data() + pk_offset, key.size() - pk_offset};
                                    if (dedupe_index_pks_) {
                                        std::string pk_key{reinterpret_cast<const char*>(pk_bytes.data()), pk_bytes.size()};
                                        if (!seen_index_pks_.insert(std::move(pk_key)).second) { continue; }
                                    }
                                    if (!table_->get_by_pk_bytes(pk_bytes, entry)) { continue; }
                                }
                                pending_ = std::move(entry);
                                return;
                            }
                            if (range_index_ >= ranges_.size()) { return; }
                            open_next_range();
                        }
                    }

                    const PackedTable* table_;
                    QuerySourceKind kind_;
                    size_t index_search_prefix_size_;
                    bool dynamic_index_pk_offset_ = false;
                    bool dedupe_index_pks_ = false;
                    std::unordered_set<std::string> seen_index_pks_;
                    std::vector<QueryRange> ranges_;
                    size_t range_index_ = 0;
                    std::unique_ptr<core::BufferArena> scan_arena_;
                    core::ArenaGenerator<engine::AkkEngine::ScanRecordView> rows_;
                    core::ArenaGenerator<engine::AkkEngine::ScanRecordView>::iterator it_;
                    std::optional<Entry> pending_;
            };

            template <typename Expr>
            class QueryView {
                public:
                    class Iterator {
                        public:
                            struct Sentinel {};

                            using value_type = Entry;
                            using difference_type = std::ptrdiff_t;
                            using iterator_category = std::input_iterator_tag;

                            [[nodiscard]] const Entry& operator*() const noexcept { return *current_; }
                            [[nodiscard]] const Entry* operator->() const noexcept { return &*current_; }

                            Iterator& operator++() {
                                advance();
                                return *this;
                            }

                            [[nodiscard]] bool operator!=(const Sentinel&) const noexcept { return current_.has_value(); }
                            [[nodiscard]] bool operator==(const Sentinel&) const noexcept { return !current_.has_value(); }

                        private:
                            friend class QueryView;

                            Iterator(QuerySource source, Expr expr, std::optional<size_t> limit)
                                : source_{std::move(source)}, expr_{std::move(expr)}, limit_{limit} { advance(); }

                            void advance() {
                                current_.reset();
                                while (source_.has_next()) {
                                    if (limit_ && matched_ >= *limit_) { return; }
                                    auto entry = source_.next();
                                    if (query::eval(expr_, entry.value)) {
                                        current_ = std::move(entry);
                                        ++matched_;
                                        return;
                                    }
                                }
                            }

                            QuerySource source_;
                            Expr expr_;
                            std::optional<size_t> limit_;
                            size_t matched_ = 0;
                            std::optional<Entry> current_;
                    };

                    [[nodiscard]] Iterator begin() const {
                        QueryPlan plan;
                        table_->make_query_plan(expr_, plan);
                        return Iterator{
                            QuerySource{table_, std::move(plan)},
                            expr_,
                            limit_
                        };
                    }

                    [[nodiscard]] typename Iterator::Sentinel end() const noexcept { return {}; }

                    template <typename Pred>
                    [[nodiscard]] auto where(Pred&& predicate) const {
                        auto next = std::forward<Pred>(predicate)(query::make_proxy<Entity>());
                        using NextExpr = decltype(next);
                        return QueryView<query::Logical<query::Op::And, Expr, NextExpr>>{
                            table_,
                            query::Logical<query::Op::And, Expr, NextExpr>{expr_, std::move(next)},
                            limit_
                        };
                    }

                    [[nodiscard]] QueryView limit(size_t n) const {
                        QueryView out{*this};
                        out.limit_ = n;
                        return out;
                    }

                    [[nodiscard]] std::optional<Entry> first() const {
                        auto it = begin();
                        if (it != end()) { return *it; }
                        return std::nullopt;
                    }

                    [[nodiscard]] bool any() const { return first().has_value(); }

                    [[nodiscard]] size_t count() const {
                        size_t n = 0;
                        auto it = begin();
                        while (it != end()) {
                            ++n;
                            ++it;
                        }
                        return n;
                    }

                    [[nodiscard]] std::vector<Entry> to_vector() const {
                        std::vector<Entry> out;
                        for (const auto& entry : *this) { out.push_back(entry); }
                        return out;
                    }

                private:
                    friend class PackedTable;

                    QueryView(const PackedTable* table, Expr expr, std::optional<size_t> limit = std::nullopt)
                        : table_{table}, expr_{std::move(expr)}, limit_{limit} {}

                    const PackedTable* table_;
                    Expr expr_;
                    std::optional<size_t> limit_;
            };

            [[nodiscard]] QueryView<query::AlwaysTrue> query() const { return QueryView<query::AlwaysTrue>{this, query::AlwaysTrue{}}; }

            template <typename Pred>
            [[nodiscard]] auto query(Pred&& predicate) const { return query().where(std::forward<Pred>(predicate)); }

            [[nodiscard]] std::string_view table_name() const noexcept { return table_name_; }
            [[nodiscard]] engine::AkkEngine& engine() noexcept { return *engine_; }
            [[nodiscard]] const engine::AkkEngine& engine() const noexcept { return *engine_; }

            template <auto FieldPtr>
            class Index {
                public:
                    using Field = binpack::detail::member_of<FieldPtr>;

                    class FindRange {
                        public:
                            FindRange(FindRange&&) noexcept = default;
                            FindRange& operator=(FindRange&&) noexcept = default;
                            FindRange(const FindRange&) = delete;
                            FindRange& operator=(const FindRange&) = delete;

                            [[nodiscard]] bool has_next() const noexcept { return pending_.has_value(); }

                            [[nodiscard]] Entry next() {
                                if (!pending_) { throw std::out_of_range("PackedTable::Index::FindRange: no next entry"); }
                                Entry out = std::move(*pending_);
                                advance();
                                return out;
                            }

                        private:
                            friend class Index;

                            FindRange(PackedTable* table, size_t search_prefix_size, std::span<const uint8_t> start_key, std::span<const uint8_t> end_key)
                                : table_{table},
                                  search_prefix_size_{search_prefix_size},
                                  scan_arena_{std::make_unique<core::BufferArena>()},
                                  rows_{table_->engine_->scan(*scan_arena_, start_key, end_key)},
                                  it_{rows_.begin()} { advance(); }

                            void advance() {
                                pending_.reset();
                                while (!(it_ == rows_.end())) {
                                    const auto& raw = *it_;
                                    const auto key = raw.key;
                                    if (key.size() <= search_prefix_size_) {
                                        ++it_;
                                        continue;
                                    }

                                    std::span pk_bytes{key.data() + search_prefix_size_, key.size() - search_prefix_size_};
                                    Entry entry;
                                    if (table_->get_by_pk_bytes(pk_bytes, entry)) {
                                        pending_ = std::move(entry);
                                        ++it_;
                                        return;
                                    }
                                    ++it_;
                                }
                            }

                            PackedTable* table_;
                            size_t search_prefix_size_;
                            std::unique_ptr<core::BufferArena> scan_arena_;
                            core::ArenaGenerator<engine::AkkEngine::ScanRecordView> rows_;
                            core::ArenaGenerator<engine::AkkEngine::ScanRecordView>::iterator it_;
                            std::optional<Entry> pending_;
                    };

                    [[nodiscard]] FindRange find(const Field& value) const {
                        table_->reset_temp_buffers();
                        table_->field_buffer_.clear();
                        table_->encode_index_field_value(value, table_->field_buffer_);
                        table_->make_index_search_prefix(prefix_, table_->field_buffer_, table_->scan_start_buffer_);
                        table_->scan_end_buffer_ = table_->scan_start_buffer_;
                        if (!detail::increment_be_bytes(table_->scan_end_buffer_.data(), table_->scan_end_buffer_.size())) { table_->scan_end_buffer_.clear(); }
                        return FindRange{table_, table_->scan_start_buffer_.size(), table_->scan_start_buffer_, table_->scan_end_buffer_};
                    }

                private:
                    friend class PackedTable;
                    Index(PackedTable* table, std::array<uint8_t, 8> prefix) : table_{table}, prefix_{prefix} {}

                    PackedTable* table_;
                    std::array<uint8_t, 8> prefix_;
            };

        private:
            friend class AkkaraDB;

            struct IndexDef {
                std::array<uint8_t, 8> prefix;
                std::string field_name;
                void (*encode_field)(const Entity&, ArenaByteBuffer&);
            };

            engine::AkkEngine* engine_ = nullptr;
            std::string table_name_;
            std::array<uint8_t, 8> pk_prefix_{};
            std::vector<IndexDef> indexes_;

            mutable std::unique_ptr<core::BufferArena> temp_arena_ = std::make_unique<core::BufferArena>();
            mutable ArenaByteBuffer pk_key_buffer_{temp_arena_.get()};
            mutable ArenaByteBuffer value_buffer_{temp_arena_.get()};
            mutable ArenaByteBuffer index_key_buffer_{temp_arena_.get()};
            mutable ArenaByteBuffer field_buffer_{temp_arena_.get()};
            mutable ArenaByteBuffer scan_start_buffer_{temp_arena_.get()};
            mutable ArenaByteBuffer scan_end_buffer_{temp_arena_.get()};

            PackedTable() = default;

            void reset_temp_buffers() const {
                temp_arena_->reset();
                pk_key_buffer_.bind(temp_arena_.get());
                value_buffer_.bind(temp_arena_.get());
                index_key_buffer_.bind(temp_arena_.get());
                field_buffer_.bind(temp_arena_.get());
                scan_start_buffer_.bind(temp_arena_.get());
                scan_end_buffer_.bind(temp_arena_.get());
            }

            template <typename Expr>
            void make_query_plan(const Expr& expr, QueryPlan& plan) const {
                reset_temp_buffers();
                if (try_make_index_plan(expr, plan)) { return; }

                plan.kind = QuerySourceKind::Table;
                plan.ranges.clear();
                make_prefix_start_end(pk_prefix_, scan_start_buffer_, scan_end_buffer_);
                add_query_range(plan, scan_start_buffer_, scan_end_buffer_, 0);
            }

            void add_query_range(
                QueryPlan& plan,
                std::span<const uint8_t> start_key,
                std::span<const uint8_t> end_key,
                size_t index_search_prefix_size,
                bool dynamic_index_pk_offset = false,
                bool dedupe_index_pks = false
            ) const {
                QueryRange range;
                range.start_key.assign(start_key.begin(), start_key.end());
                range.end_key.assign(end_key.begin(), end_key.end());
                range.index_search_prefix_size = index_search_prefix_size;
                range.dynamic_index_pk_offset = dynamic_index_pk_offset;
                range.dedupe_index_pks = dedupe_index_pks;
                plan.ranges.push_back(std::move(range));
            }

            template <typename Expr>
            [[nodiscard]] bool try_make_index_plan(const Expr& expr, QueryPlan& plan) const {
                using E = std::remove_cvref_t<Expr>;
                if constexpr (query::is_and_v<E>) { return try_make_index_plan(expr.lhs, plan) || try_make_index_plan(expr.rhs, plan); }
                else if constexpr (query::is_compare_v<E>) { return try_make_compare_index_plan(expr, plan); }
                else { return false; }
            }

            template <query::Op Operator, typename L, typename R>
            [[nodiscard]] bool try_make_compare_index_plan(const query::Compare<Operator, L, R>& expr, QueryPlan& plan) const {
                if constexpr (query::is_column_v<L> && query::is_literal_v<R>) {
                    return try_make_field_index_plan<Operator, std::remove_cvref_t<L>::field_ptr>(query::literal_value(expr.rhs), plan);
                }
                else if constexpr (query::is_literal_v<L> && query::is_column_v<R>) {
                    return try_make_field_index_plan<query::swapped_compare_op<Operator>, std::remove_cvref_t<R>::field_ptr>(
                        query::literal_value(expr.lhs),
                        plan
                    );
                }
                else { return false; }
            }

            template <auto FieldPtr>
            [[nodiscard]] const IndexDef* find_index_def_for() const {
                const std::string_view field_name = binpack::detail::member_name<FieldPtr>();
                for (const auto& idx : indexes_) { if (idx.field_name == field_name) { return &idx; } }
                return nullptr;
            }

            template <typename Field, typename Lit>
            [[nodiscard]] static bool literal_to_field(const Lit& literal, Field& out) {
                if constexpr (std::is_same_v<Field, std::string>) {
                    if constexpr (std::is_convertible_v<Lit, std::string_view>) {
                        out = std::string{std::string_view{literal}};
                        return true;
                    }
                    else { return false; }
                }
                else if constexpr (std::is_arithmetic_v<Field> && std::is_arithmetic_v<Lit>) {
                    if constexpr (std::is_unsigned_v<Field> && std::is_signed_v<Lit>) { if (literal < 0) { return false; } }
                    const auto value = static_cast<long double>(literal);
                    if (value < static_cast<long double>(std::numeric_limits<Field>::lowest()) || value > static_cast<long double>(std::numeric_limits<
                        Field>::max())) { return false; }
                    out = static_cast<Field>(literal);
                    return true;
                }
                else if constexpr (std::is_constructible_v<Field, Lit>) {
                    out = Field{literal};
                    return true;
                }
                else if constexpr (std::is_convertible_v<Lit, Field>) {
                    out = literal;
                    return true;
                }
                else { return false; }
            }

            [[nodiscard]] static bool like_pattern_to_prefix(std::string_view pattern, std::string_view& prefix) {
                const size_t wildcard = pattern.find_first_of("%_");
                if (wildcard == std::string_view::npos) {
                    prefix = pattern;
                    return true;
                }
                if (pattern[wildcard] != '%' || wildcard + 1 != pattern.size()) { return false; }
                prefix = pattern.substr(0, wildcard);
                return true;
            }

            [[nodiscard]] static uint32_t read_be32(const uint8_t* src) noexcept {
                return (static_cast<uint32_t>(src[0]) << 24) |
                    (static_cast<uint32_t>(src[1]) << 16) |
                    (static_cast<uint32_t>(src[2]) << 8) |
                    static_cast<uint32_t>(src[3]);
            }

            template <typename Field>
            static void encode_index_field_value(const Field& value, ArenaByteBuffer& out) {
                binpack::BinPack::encode_into(value, out);
            }

            [[nodiscard]] bool try_make_string_prefix_index_plan(const std::array<uint8_t, 8>& index_prefix, std::string_view prefix, QueryPlan& plan) const {
                (void)prefix;
                make_prefix_start_end(index_prefix, scan_start_buffer_, scan_end_buffer_);
                plan.kind = QuerySourceKind::Index;
                plan.ranges.clear();
                add_query_range(plan, scan_start_buffer_, scan_end_buffer_, 0, true, true);
                return true;
            }

            template <query::Op Operator, auto FieldPtr, typename Lit>
            [[nodiscard]] bool try_make_field_index_plan(const Lit& literal, QueryPlan& plan) const {
                using Field = binpack::detail::member_of<FieldPtr>;
                const IndexDef* idx = find_index_def_for<FieldPtr>();
                if (idx == nullptr) { return false; }

                if constexpr (Operator == query::Op::Eq) {
                    Field value{};
                    if (!literal_to_field<Field>(literal, value)) { return false; }
                    field_buffer_.clear();
                    encode_index_field_value(value, field_buffer_);
                    make_index_search_prefix(idx->prefix, field_buffer_, scan_start_buffer_);
                    scan_end_buffer_ = scan_start_buffer_;
                    if (!detail::increment_be_bytes(scan_end_buffer_.data(), scan_end_buffer_.size())) { scan_end_buffer_.clear(); }
                    plan.kind = QuerySourceKind::Index;
                    plan.ranges.clear();
                    add_query_range(plan, scan_start_buffer_, scan_end_buffer_, scan_start_buffer_.size());
                    return true;
                }
                else if constexpr (Operator == query::Op::In) {
                    return false;
                }
                else if constexpr (Operator == query::Op::StartsWith) {
                    return false;
                }
                else if constexpr (Operator == query::Op::Like) {
                    return false;
                }
                else if constexpr ((Operator == query::Op::Gt || Operator == query::Op::Ge || Operator == query::Op::Lt || Operator == query::Op::Le) &&
                    std::is_integral_v<Field> && std::is_unsigned_v<Field>) {
                    Field value{};
                    if (!literal_to_field<Field>(literal, value)) { return false; }
                    make_prefix_start_end(idx->prefix, scan_start_buffer_, scan_end_buffer_);
                    field_buffer_.clear();

                    if constexpr (Operator == query::Op::Gt) {
                        if (value == std::numeric_limits<Field>::max()) { return false; }
                        const Field lower = static_cast<Field>(value + 1);
                        encode_index_field_value(lower, field_buffer_);
                        make_index_search_prefix(idx->prefix, field_buffer_, scan_start_buffer_);
                    }
                    else if constexpr (Operator == query::Op::Ge) {
                        encode_index_field_value(value, field_buffer_);
                        make_index_search_prefix(idx->prefix, field_buffer_, scan_start_buffer_);
                    }
                    else if constexpr (Operator == query::Op::Lt) {
                        if (value == std::numeric_limits<Field>::lowest()) { return false; }
                        encode_index_field_value(value, field_buffer_);
                        make_index_search_prefix(idx->prefix, field_buffer_, scan_end_buffer_);
                    }
                    else if constexpr (Operator == query::Op::Le) {
                        if (value != std::numeric_limits<Field>::max()) {
                            const Field upper = static_cast<Field>(value + 1);
                            encode_index_field_value(upper, field_buffer_);
                            make_index_search_prefix(idx->prefix, field_buffer_, scan_end_buffer_);
                        }
                        else { encode_index_field_value(value, field_buffer_); }
                    }

                    plan.kind = QuerySourceKind::Index;
                    plan.ranges.clear();
                    add_query_range(plan, scan_start_buffer_, scan_end_buffer_, 12 + field_buffer_.size());
                    return true;
                }
                else { return false; }
            }

            void make_pk_key(const PK& pk, ArenaByteBuffer& out) const {
                out.clear();
                if constexpr (std::is_integral_v<PK> && sizeof(PK) <= 8) {
                    out.resize(8 + sizeof(PK));
                    std::memcpy(out.data(), pk_prefix_.data(), pk_prefix_.size());
                    uint64_t v = static_cast<uint64_t>(std::make_unsigned_t<PK>(pk));
                    for (size_t i = 0; i < sizeof(PK); ++i) { out[8 + i] = static_cast<uint8_t>(v >> ((sizeof(PK) - 1 - i) * 8)); }
                }
                else {
                    out.reserve(8 + binpack::BinPack::estimate_size(pk));
                    out.insert(out.end(), pk_prefix_.begin(), pk_prefix_.end());
                    binpack::BinPack::encode_into(pk, out);
                }
            }

            [[nodiscard]] bool get_by_pk_bytes(std::span<const uint8_t> pk_bytes, Entry& out) const {
                pk_key_buffer_.clear();
                pk_key_buffer_.reserve(8 + pk_bytes.size());
                pk_key_buffer_.insert(pk_key_buffer_.end(), pk_prefix_.begin(), pk_prefix_.end());
                pk_key_buffer_.insert(pk_key_buffer_.end(), pk_bytes.begin(), pk_bytes.end());

                std::span<const uint8_t> value_span;
                if (!engine_->get_into_arena(pk_key_buffer_, *temp_arena_, value_span)) { return false; }
                std::span pk_span{pk_bytes.data(), pk_bytes.size()};
                out = Entry{binpack::BinPack::decode<PK>(pk_span), binpack::BinPack::decode<Entity>(value_span)};
                return true;
            }

            void put_hinted(std::span<const uint8_t> key, std::span<const uint8_t> value) {
                engine_->put_hinted(key, value, compute_key_fp64(key), build_mini_key(key));
            }

            void remove_hinted(std::span<const uint8_t> key) { engine_->remove_hinted(key, compute_key_fp64(key), build_mini_key(key)); }

            static void make_prefix_start_end(const std::array<uint8_t, 8>& prefix, ArenaByteBuffer& start, ArenaByteBuffer& end) {
                start.assign(prefix.begin(), prefix.end());
                end.assign(prefix.begin(), prefix.end());
                if (!detail::increment_be_bytes(end.data(), end.size())) { end.clear(); }
            }

            static std::array<uint8_t, 8> make_table_prefix(std::string_view name) {
                std::array<uint8_t, 8> out{};
                detail::write_be64(detail::fnv1a_64(name), out.data());
                return out;
            }

            static std::array<uint8_t, 8> make_index_prefix(std::string_view table_name, std::string_view field_name) {
                std::string input;
                input.reserve(table_name.size() + 5 + field_name.size());
                input.append(table_name);
                input.append(":idx:");
                input.append(field_name);
                std::array<uint8_t, 8> out{};
                detail::write_be64(detail::fnv1a_64(input), out.data());
                return out;
            }

            void make_index_search_prefix(const std::array<uint8_t, 8>& prefix, std::span<const uint8_t> field_bytes, ArenaByteBuffer& out) const {
                out.clear();
                out.resize(12 + field_bytes.size());
                std::memcpy(out.data(), prefix.data(), prefix.size());
                detail::write_be32(static_cast<uint32_t>(field_bytes.size()), out.data() + 8);
                if (!field_bytes.empty()) { std::memcpy(out.data() + 12, field_bytes.data(), field_bytes.size()); }
            }

            void make_index_key(
                const std::array<uint8_t, 8>& prefix,
                std::span<const uint8_t> field_bytes,
                std::span<const uint8_t> pk_key,
                ArenaByteBuffer& out
            ) const {
                if (pk_key.size() < pk_prefix_.size()) { throw std::invalid_argument("PackedTable: malformed primary key"); }
                make_index_search_prefix(prefix, field_bytes, out);
                out.insert(out.end(), pk_key.begin() + static_cast<std::ptrdiff_t>(pk_prefix_.size()), pk_key.end());
            }

            void write_index_entries(const Entity& entity, std::span<const uint8_t> pk_key) {
                constexpr std::span<const uint8_t> empty_value{};
                for (const auto& idx : indexes_) {
                    idx.encode_field(entity, field_buffer_);
                    make_index_key(idx.prefix, field_buffer_, pk_key, index_key_buffer_);
                    put_hinted(index_key_buffer_, empty_value);
                }
            }

            void remove_index_entries(const Entity& entity, std::span<const uint8_t> pk_key) {
                for (const auto& idx : indexes_) {
                    idx.encode_field(entity, field_buffer_);
                    make_index_key(idx.prefix, field_buffer_, pk_key, index_key_buffer_);
                    remove_hinted(index_key_buffer_);
                }
            }
    };
} // namespace akkaradb

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
#include "akk/engine/AkkEngine.hpp"
#include "akk/core/buffer/BufferArena.hpp"
#include "akk/core/record/KeyFingerprint.hpp"

#include <array>
#include <bit>
#include <concepts>
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
#include <typeindex>
#include <unordered_set>
#include <utility>
#include <vector>

namespace akkaradb {
    namespace query {
        template <typename Entity>
        struct ProxyTag {};

        enum class Op {
            EQ,
            NE,
            GT,
            GE,
            LT,
            LE,
            AND,
            OR,
            IN_LIST,
            NOT_IN,
            STARTS_WITH,
            CONTAINS,
            LIKE,
            IS_NULL,
            IS_NOT_NULL
        };

        struct AlwaysTrue {};

        template <Op Operator, typename L, typename R>
        struct Compare;

        template <typename Parent, auto FieldPtr>
        struct FieldPath;

        template <typename Map, typename Key>
        struct MapGet;

        template <auto FieldPtr>
        struct Column {
            static constexpr auto fieldPtr = FieldPtr;

            template <auto NestedFieldPtr>
            [[nodiscard]] auto field() const;

            template <typename R>
            [[nodiscard]] auto mapGet(R&& key) const;

            template <typename R>
            [[nodiscard]] auto get(R&& key) const;

            template <typename R>
            [[nodiscard]] auto in(R&& rhs) const;

            template <typename T>
            [[nodiscard]] auto in(std::initializer_list<T> rhs) const;

            template <typename R>
            [[nodiscard]] auto notIn(R&& rhs) const;

            template <typename T>
            [[nodiscard]] auto notIn(std::initializer_list<T> rhs) const;

            template <typename R>
            [[nodiscard]] auto startsWith(R&& rhs) const;

            template <typename R>
            [[nodiscard]] auto contains(R&& rhs) const;

            template <typename R>
            [[nodiscard]] auto like(R&& rhs) const;

            [[nodiscard]] auto isNull() const;

            [[nodiscard]] auto isNotNull() const;
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

        template <Op Operator, typename X>
        struct Unary {
            static constexpr Op op = Operator;
            X x;
        };

        template <typename Parent, auto FieldPtr>
        struct FieldPath {
            static constexpr auto fieldPtr = FieldPtr;
            Parent parent;

            template <auto NestedFieldPtr>
            [[nodiscard]] auto field() const;

            template <typename R>
            [[nodiscard]] auto mapGet(R&& key) const;

            template <typename R>
            [[nodiscard]] auto get(R&& key) const;

            [[nodiscard]] auto isNull() const;

            [[nodiscard]] auto isNotNull() const;
        };

        template <typename Map, typename Key>
        struct MapGet {
            Map map;
            Key key;

            template <typename R>
            [[nodiscard]] auto mapGet(R&& nestedKey) const;

            template <typename R>
            [[nodiscard]] auto get(R&& nestedKey) const;

            [[nodiscard]] auto isNull() const;

            [[nodiscard]] auto isNotNull() const;
        };

        template <typename T>
        struct IsExpr : std::false_type {};

        template <>
        struct IsExpr<AlwaysTrue> : std::true_type {};

        template <auto FieldPtr>
        struct IsExpr<Column<FieldPtr>> : std::true_type {};

        template <typename Parent, auto FieldPtr>
        struct IsExpr<FieldPath<Parent, FieldPtr>> : std::true_type {};

        template <typename Map, typename Key>
        struct IsExpr<MapGet<Map, Key>> : std::true_type {};

        template <typename T>
        struct IsExpr<Literal<T>> : std::true_type {};

        template <Op Operator, typename L, typename R>
        struct IsExpr<Compare<Operator, L, R>> : std::true_type {};

        template <Op Operator, typename L, typename R>
        struct IsExpr<Logical<Operator, L, R>> : std::true_type {};

        template <typename X>
        struct IsExpr<Not<X>> : std::true_type {};

        template <Op Operator, typename X>
        struct IsExpr<Unary<Operator, X>> : std::true_type {};

        template <typename T>
        inline constexpr bool isExpr = IsExpr<std::remove_cvref_t<T>>::value;

        template <typename T>
        using LiteralStorage = std::conditional_t<std::is_convertible_v<T, std::string_view> && !std::is_arithmetic_v<std::remove_cvref_t<
            T>>, std::string, std::remove_cvref_t<T>>;

        template <typename T>
        [[nodiscard]] auto normalizeLiteral(T&& value) {
            if constexpr (std::is_convertible_v<T, std::string_view> && !std::is_arithmetic_v<std::remove_cvref_t<T>>) {
                return std::string{std::string_view{value}};
            }
            else { return std::forward<T>(value); }
        }

        template <typename T>
        [[nodiscard]] auto asExpr(T&& value) {
            if constexpr (isExpr<T>) { return std::forward<T>(value); }
            else { return Literal<LiteralStorage<T>>{normalizeLiteral(std::forward<T>(value))}; }
        }

        template <typename L, typename R> requires(isExpr<L> || isExpr<R>)
        [[nodiscard]] auto operator==(L&& lhs, R&& rhs) {
            return Compare<Op::EQ, decltype(asExpr(std::forward<L>(lhs))), decltype(asExpr(std::forward<R>(rhs)))>{
                asExpr(std::forward<L>(lhs)),
                asExpr(std::forward<R>(rhs))
            };
        }

        template <typename L, typename R> requires(isExpr<L> || isExpr<R>)
        [[nodiscard]] auto operator!=(L&& lhs, R&& rhs) {
            return Compare<Op::NE, decltype(asExpr(std::forward<L>(lhs))), decltype(asExpr(std::forward<R>(rhs)))>{
                asExpr(std::forward<L>(lhs)),
                asExpr(std::forward<R>(rhs))
            };
        }

        template <typename L, typename R> requires(isExpr<L> || isExpr<R>)
        [[nodiscard]] auto operator>(L&& lhs, R&& rhs) {
            return Compare<Op::GT, decltype(asExpr(std::forward<L>(lhs))), decltype(asExpr(std::forward<R>(rhs)))>{
                asExpr(std::forward<L>(lhs)),
                asExpr(std::forward<R>(rhs))
            };
        }

        template <typename L, typename R> requires(isExpr<L> || isExpr<R>)
        [[nodiscard]] auto operator>=(L&& lhs, R&& rhs) {
            return Compare<Op::GE, decltype(asExpr(std::forward<L>(lhs))), decltype(asExpr(std::forward<R>(rhs)))>{
                asExpr(std::forward<L>(lhs)),
                asExpr(std::forward<R>(rhs))
            };
        }

        template <typename L, typename R> requires(isExpr<L> || isExpr<R>)
        [[nodiscard]] auto operator<(L&& lhs, R&& rhs) {
            return Compare<Op::LT, decltype(asExpr(std::forward<L>(lhs))), decltype(asExpr(std::forward<R>(rhs)))>{
                asExpr(std::forward<L>(lhs)),
                asExpr(std::forward<R>(rhs))
            };
        }

        template <typename L, typename R> requires(isExpr<L> || isExpr<R>)
        [[nodiscard]] auto operator<=(L&& lhs, R&& rhs) {
            return Compare<Op::LE, decltype(asExpr(std::forward<L>(lhs))), decltype(asExpr(std::forward<R>(rhs)))>{
                asExpr(std::forward<L>(lhs)),
                asExpr(std::forward<R>(rhs))
            };
        }

        template <typename L, typename R> requires(isExpr<L> && isExpr<R>)
        [[nodiscard]] auto operator&&(L&& lhs, R&& rhs) {
            return Logical<Op::AND, std::remove_cvref_t<L>, std::remove_cvref_t<R>>{std::forward<L>(lhs), std::forward<R>(rhs)};
        }

        template <typename L, typename R> requires(isExpr<L> && isExpr<R>)
        [[nodiscard]] auto operator||(L&& lhs, R&& rhs) {
            return Logical<Op::OR, std::remove_cvref_t<L>, std::remove_cvref_t<R>>{std::forward<L>(lhs), std::forward<R>(rhs)};
        }

        template <typename X> requires(isExpr<X>)
        [[nodiscard]] auto operator!(X&& x) { return Not<std::remove_cvref_t<X>>{std::forward<X>(x)}; }

        template <typename L, typename R> requires(isExpr<L> || isExpr<R>)
        [[nodiscard]] auto in(L&& lhs, R&& rhs) {
            return Compare<Op::IN_LIST, decltype(asExpr(std::forward<L>(lhs))), decltype(asExpr(std::forward<R>(rhs)))>{
                asExpr(std::forward<L>(lhs)),
                asExpr(std::forward<R>(rhs))
            };
        }

        template <typename L, typename T> requires(isExpr<L>)
        [[nodiscard]] auto in(L&& lhs, std::initializer_list<T> rhs) {
            return in(std::forward<L>(lhs), std::vector<LiteralStorage<T>>{rhs.begin(), rhs.end()});
        }

        template <typename L, typename R> requires(isExpr<L> || isExpr<R>)
        [[nodiscard]] auto notIn(L&& lhs, R&& rhs) {
            return Compare<Op::NOT_IN, decltype(asExpr(std::forward<L>(lhs))), decltype(asExpr(std::forward<R>(rhs)))>{
                asExpr(std::forward<L>(lhs)),
                asExpr(std::forward<R>(rhs))
            };
        }

        template <typename L, typename T> requires(isExpr<L>)
        [[nodiscard]] auto notIn(L&& lhs, std::initializer_list<T> rhs) {
            return notIn(std::forward<L>(lhs), std::vector<LiteralStorage<T>>{rhs.begin(), rhs.end()});
        }

        template <typename L, typename R> requires(isExpr<L> || isExpr<R>)
        [[nodiscard]] auto startsWith(L&& lhs, R&& rhs) {
            return Compare<Op::STARTS_WITH, decltype(asExpr(std::forward<L>(lhs))), decltype(asExpr(std::forward<R>(rhs)))>{
                asExpr(std::forward<L>(lhs)),
                asExpr(std::forward<R>(rhs))
            };
        }

        template <typename L, typename R> requires(isExpr<L> || isExpr<R>)
        [[nodiscard]] auto contains(L&& lhs, R&& rhs) {
            return Compare<Op::CONTAINS, decltype(asExpr(std::forward<L>(lhs))), decltype(asExpr(std::forward<R>(rhs)))>{
                asExpr(std::forward<L>(lhs)),
                asExpr(std::forward<R>(rhs))
            };
        }

        template <typename L, typename R> requires(isExpr<L> || isExpr<R>)
        [[nodiscard]] auto like(L&& lhs, R&& rhs) {
            return Compare<Op::LIKE, decltype(asExpr(std::forward<L>(lhs))), decltype(asExpr(std::forward<R>(rhs)))>{
                asExpr(std::forward<L>(lhs)),
                asExpr(std::forward<R>(rhs))
            };
        }

        template <typename X> requires(isExpr<X>)
        [[nodiscard]] auto isNull(X&& x) { return Unary<Op::IS_NULL, std::remove_cvref_t<X>>{std::forward<X>(x)}; }

        template <typename X> requires(isExpr<X>)
        [[nodiscard]] auto isNotNull(X&& x) { return Unary<Op::IS_NOT_NULL, std::remove_cvref_t<X>>{std::forward<X>(x)}; }

        template <auto FieldPtr, typename X> requires(isExpr<X>)
        [[nodiscard]] auto field(X&& x) { return FieldPath<std::remove_cvref_t<X>, FieldPtr>{std::forward<X>(x)}; }

        template <typename M, typename K> requires(isExpr<M>)
        [[nodiscard]] auto mapGet(M&& map, K&& key) {
            return MapGet<std::remove_cvref_t<M>, decltype(asExpr(std::forward<K>(key)))>{
                std::forward<M>(map),
                asExpr(std::forward<K>(key))
            };
        }

        template <auto FieldPtr>
        template <auto NestedFieldPtr>
        [[nodiscard]] auto Column<FieldPtr>::field() const { return query::field<NestedFieldPtr>(*this); }

        template <auto FieldPtr>
        template <typename R>
        [[nodiscard]] auto Column<FieldPtr>::mapGet(R&& key) const { return query::mapGet(*this, std::forward<R>(key)); }

        template <auto FieldPtr>
        template <typename R>
        [[nodiscard]] auto Column<FieldPtr>::get(R&& key) const { return query::mapGet(*this, std::forward<R>(key)); }

        template <typename Parent, auto FieldPtr>
        template <auto NestedFieldPtr>
        [[nodiscard]] auto FieldPath<Parent, FieldPtr>::field() const { return query::field<NestedFieldPtr>(*this); }

        template <typename Parent, auto FieldPtr>
        template <typename R>
        [[nodiscard]] auto FieldPath<Parent, FieldPtr>::mapGet(R&& key) const { return query::mapGet(*this, std::forward<R>(key)); }

        template <typename Parent, auto FieldPtr>
        template <typename R>
        [[nodiscard]] auto FieldPath<Parent, FieldPtr>::get(R&& key) const { return query::mapGet(*this, std::forward<R>(key)); }

        template <typename Parent, auto FieldPtr>
        [[nodiscard]] auto FieldPath<Parent, FieldPtr>::isNull() const { return query::isNull(*this); }

        template <typename Parent, auto FieldPtr>
        [[nodiscard]] auto FieldPath<Parent, FieldPtr>::isNotNull() const { return query::isNotNull(*this); }

        template <typename Map, typename Key>
        template <typename R>
        [[nodiscard]] auto MapGet<Map, Key>::mapGet(R&& nestedKey) const { return query::mapGet(*this, std::forward<R>(nestedKey)); }

        template <typename Map, typename Key>
        template <typename R>
        [[nodiscard]] auto MapGet<Map, Key>::get(R&& nestedKey) const { return query::mapGet(*this, std::forward<R>(nestedKey)); }

        template <typename Map, typename Key>
        [[nodiscard]] auto MapGet<Map, Key>::isNull() const { return query::isNull(*this); }

        template <typename Map, typename Key>
        [[nodiscard]] auto MapGet<Map, Key>::isNotNull() const { return query::isNotNull(*this); }

        template <auto FieldPtr>
        template <typename R>
        [[nodiscard]] auto Column<FieldPtr>::in(R&& rhs) const { return query::in(*this, std::forward<R>(rhs)); }

        template <auto FieldPtr>
        template <typename T>
        [[nodiscard]] auto Column<FieldPtr>::in(std::initializer_list<T> rhs) const { return query::in(*this, rhs); }

        template <auto FieldPtr>
        template <typename R>
        [[nodiscard]] auto Column<FieldPtr>::notIn(R&& rhs) const { return query::notIn(*this, std::forward<R>(rhs)); }

        template <auto FieldPtr>
        template <typename T>
        [[nodiscard]] auto Column<FieldPtr>::notIn(std::initializer_list<T> rhs) const { return query::notIn(*this, rhs); }

        template <auto FieldPtr>
        template <typename R>
        [[nodiscard]] auto Column<FieldPtr>::startsWith(R&& rhs) const { return query::startsWith(*this, std::forward<R>(rhs)); }

        template <auto FieldPtr>
        template <typename R>
        [[nodiscard]] auto Column<FieldPtr>::contains(R&& rhs) const { return query::contains(*this, std::forward<R>(rhs)); }

        template <auto FieldPtr>
        template <typename R>
        [[nodiscard]] auto Column<FieldPtr>::like(R&& rhs) const { return query::like(*this, std::forward<R>(rhs)); }

        template <auto FieldPtr>
        [[nodiscard]] auto Column<FieldPtr>::isNull() const { return query::isNull(*this); }

        template <auto FieldPtr>
        [[nodiscard]] auto Column<FieldPtr>::isNotNull() const { return query::isNotNull(*this); }

        template <typename Entity>
        [[nodiscard]] auto makeProxy() { return akkaradbQueryProxy(ProxyTag<Entity>{}); }

        template <typename Entity>
        using QueryProxy = decltype(makeProxy<Entity>());

        template <typename T>
        struct IsColumn : std::false_type {};

        template <auto FieldPtr>
        struct IsColumn<Column<FieldPtr>> : std::true_type {};

        template <typename T>
        inline constexpr bool isColumn = IsColumn<std::remove_cvref_t<T>>::value;

        template <typename T>
        struct IsLiteral : std::false_type {};

        template <typename T>
        struct IsLiteral<Literal<T>> : std::true_type {};

        template <typename T>
        inline constexpr bool isLiteral = IsLiteral<std::remove_cvref_t<T>>::value;

        template <typename T>
        struct IsCompare : std::false_type {};

        template <Op Operator, typename L, typename R>
        struct IsCompare<Compare<Operator, L, R>> : std::true_type {};

        template <typename T>
        inline constexpr bool isCompare = IsCompare<std::remove_cvref_t<T>>::value;

        template <typename T>
        struct IsNot : std::false_type {};

        template <typename X>
        struct IsNot<Not<X>> : std::true_type {};

        template <typename T>
        inline constexpr bool isNot = IsNot<std::remove_cvref_t<T>>::value;

        template <typename T>
        struct IsUnary : std::false_type {};

        template <Op Operator, typename X>
        struct IsUnary<Unary<Operator, X>> : std::true_type {};

        template <typename T>
        inline constexpr bool isUnary = IsUnary<std::remove_cvref_t<T>>::value;

        template <typename T>
        struct IsOptional : std::false_type {};

        template <typename T>
        struct IsOptional<std::optional<T>> : std::true_type {};

        template <typename T>
        inline constexpr bool isOptional = IsOptional<std::remove_cvref_t<T>>::value;

        template <typename T>
        inline constexpr bool isStringLikeV = requires(const std::remove_cvref_t<T>& value) {
            std::string_view{value};
        };

        template <typename T>
        struct IsAnd : std::false_type {};

        template <typename L, typename R>
        struct IsAnd<Logical<Op::AND, L, R>> : std::true_type {};

        template <typename T>
        inline constexpr bool isAnd = IsAnd<std::remove_cvref_t<T>>::value;

        template <Op Operator>
        inline constexpr Op swappedCompareOp = Operator;

        template <>
        inline constexpr Op swappedCompareOp<Op::GT> = Op::LT;

        template <>
        inline constexpr Op swappedCompareOp<Op::GE> = Op::LE;

        template <>
        inline constexpr Op swappedCompareOp<Op::LT> = Op::GT;

        template <>
        inline constexpr Op swappedCompareOp<Op::LE> = Op::GE;

        template <typename T>
        [[nodiscard]] const auto& literalValue(const Literal<T>& literal) noexcept { return literal.value; }

        template <typename Entity>
        [[nodiscard]] bool eval(const AlwaysTrue&, const Entity&) { return true; }

        template <auto FieldPtr, typename Entity>
        [[nodiscard]] decltype(auto) eval(const Column<FieldPtr>&, const Entity& entity) { return (entity.*FieldPtr); }

        template <typename Parent, auto FieldPtr, typename Entity>
        [[nodiscard]] decltype(auto) eval(const FieldPath<Parent, FieldPtr>& expr, const Entity& entity) {
            return (eval(expr.parent, entity).*FieldPtr);
        }

        template <typename Map, typename Key, typename Entity>
        [[nodiscard]] auto eval(const MapGet<Map, Key>& expr, const Entity& entity) {
            const auto& map = eval(expr.map, entity);
            const auto& key = eval(expr.key, entity);
            using Value = typename std::remove_cvref_t<decltype(map)>::mapped_type;
            const auto it = map.find(key);
            if (it == map.end()) { return std::optional<Value>{}; }
            return std::optional<Value>{it->second};
        }

        template <typename T, typename Entity>
        [[nodiscard]] const T& eval(const Literal<T>& literal, const Entity&) { return literal.value; }

        template <typename Needle, typename Haystack>
        [[nodiscard]] bool containsValue(const Haystack& haystack, const Needle& needle) {
            for (const auto& value : haystack) { if (value == needle) { return true; } }
            return false;
        }

        template <typename Value, typename Pattern>
        [[nodiscard]] bool stringStartsWith(const Value& value, const Pattern& pattern) {
            std::string_view haystack{value};
            std::string_view needle{pattern};
            return haystack.starts_with(needle);
        }

        template <typename Value, typename Pattern>
        [[nodiscard]] bool stringContains(const Value& value, const Pattern& pattern) {
            std::string_view haystack{value};
            std::string_view needle{pattern};
            return haystack.find(needle) != std::string_view::npos;
        }

        [[nodiscard]] inline bool likeMatch(std::string_view value, size_t vi, std::string_view pattern, size_t pi) {
            while (pi < pattern.size()) {
                if (pattern[pi] == '%') {
                    while (pi + 1 < pattern.size() && pattern[pi + 1] == '%') { ++pi; }
                    if (pi + 1 == pattern.size()) { return true; }
                    for (size_t next = vi; next <= value.size(); ++next) { if (likeMatch(value, next, pattern, pi + 1)) { return true; } }
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
        [[nodiscard]] bool stringLike(const Value& value, const Pattern& pattern) {
            return likeMatch(std::string_view{value}, 0, std::string_view{pattern}, 0);
        }

        template <typename Value>
        [[nodiscard]] bool valueIsNull(const Value& value) {
            if constexpr (isOptional<Value>) { return !value.has_value(); }
            else {
                (void)value;
                return false;
            }
        }

        template <typename L, typename R>
        [[nodiscard]] bool valueEq(const L& lhs, const R& rhs) {
            if constexpr (std::is_integral_v<L> && std::is_integral_v<R>) { return std::cmp_equal(lhs, rhs); }
            else { return lhs == rhs; }
        }

        template <typename L, typename R>
        [[nodiscard]] bool valueLt(const L& lhs, const R& rhs) {
            if constexpr (std::is_integral_v<L> && std::is_integral_v<R>) { return std::cmp_less(lhs, rhs); }
            else { return lhs < rhs; }
        }

        template <Op Operator, typename L, typename R, typename Entity>
        [[nodiscard]] bool eval(const Compare<Operator, L, R>& expr, const Entity& entity) {
            const auto lhs = eval(expr.lhs, entity);
            const auto rhs = eval(expr.rhs, entity);
            if constexpr (Operator == Op::EQ) { return valueEq(lhs, rhs); }
            else if constexpr (Operator == Op::NE) { return !valueEq(lhs, rhs); }
            else if constexpr (Operator == Op::GT) { return valueLt(rhs, lhs); }
            else if constexpr (Operator == Op::GE) { return !valueLt(lhs, rhs); }
            else if constexpr (Operator == Op::LT) { return valueLt(lhs, rhs); }
            else if constexpr (Operator == Op::LE) { return !valueLt(rhs, lhs); }
            else if constexpr (Operator == Op::IN_LIST) { return containsValue(rhs, lhs); }
            else if constexpr (Operator == Op::NOT_IN) { return !containsValue(rhs, lhs); }
            else if constexpr (Operator == Op::STARTS_WITH) { return stringStartsWith(lhs, rhs); }
            else if constexpr (Operator == Op::CONTAINS) { return stringContains(lhs, rhs); }
            else if constexpr (Operator == Op::LIKE) { return stringLike(lhs, rhs); }
        }

        template <Op Operator, typename L, typename R, typename Entity>
        [[nodiscard]] bool eval(const Logical<Operator, L, R>& expr, const Entity& entity) {
            if constexpr (Operator == Op::AND) { return eval(expr.lhs, entity) && eval(expr.rhs, entity); }
            else { return eval(expr.lhs, entity) || eval(expr.rhs, entity); }
        }

        template <typename X, typename Entity>
        [[nodiscard]] bool eval(const Not<X>& expr, const Entity& entity) { return !eval(expr.x, entity); }

        template <Op Operator, typename X, typename Entity>
        [[nodiscard]] bool eval(const Unary<Operator, X>& expr, const Entity& entity) {
            const auto value = eval(expr.x, entity);
            if constexpr (Operator == Op::IS_NULL) { return valueIsNull(value); }
            else { return !valueIsNull(value); }
        }
    } // namespace query

    #define AKKARADB_QUERYABLE_FIELD(Type, Field) ::akkaradb::query::Column<&Type::Field> Field{};
    #define AKKARADB_QUERYABLE(Type, A, B, C, D) \
        [[nodiscard]] inline auto akkaradbQueryProxy(::akkaradb::query::ProxyTag<Type>) { \
            struct Proxy { \
                AKKARADB_QUERYABLE_FIELD(Type, A) \
                AKKARADB_QUERYABLE_FIELD(Type, B) \
                AKKARADB_QUERYABLE_FIELD(Type, C) \
                AKKARADB_QUERYABLE_FIELD(Type, D) \
            }; \
            return Proxy{}; \
        }

    #define AKKARADB_ENTITY(Type, PrimaryKey, B, C, D) \
        AKKARADB_REF_ENTITY(Type, PrimaryKey); \
        AKKARADB_QUERYABLE(Type, PrimaryKey, B, C, D)

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

                void bind(BufferArena* arena) noexcept {
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
                    ensureArena();
                    std::byte* raw = arena_->allocate(capacity, alignof(uint8_t));
                    auto* next = reinterpret_cast<uint8_t*>(raw);
                    if (data_ != nullptr && size_ != 0) { std::memcpy(next, data_, size_); }
                    data_ = next;
                    capacity_ = capacity;
                }

                void resize(size_t size) {
                    const size_t oldSize = size_;
                    reserve(size);
                    if (size > oldSize) { std::memset(data_ + oldSize, 0, size - oldSize); }
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
                void ensureArena() const { if (arena_ == nullptr) { throw std::logic_error("PackedTable: arena buffer is not bound"); } }

                BufferArena* arena_ = nullptr;
                uint8_t* data_ = nullptr;
                size_t size_ = 0;
                size_t capacity_ = 0;
        };

        public:
            using Entity = binpack::detail::classOf<PrimaryKeyPtr>;
            using PK = binpack::detail::memberOf<PrimaryKeyPtr>;

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

            template <auto FieldPtr, auto TargetPrimaryKeyPtr>
            PackedTable& bindRef(PackedTable<TargetPrimaryKeyPtr>& target) {
                static_assert(std::is_same_v<binpack::detail::classOf<FieldPtr>, Entity>, "ref field must belong to the table entity");
                using Field = binpack::detail::memberOf<FieldPtr>;
                static_assert(isRef<Field>, "bindRef field must be akkaradb::Ref<T>");
                using Target = typename RefTarget<Field>::Type;
                using TargetTable = PackedTable<TargetPrimaryKeyPtr>;
                static_assert(std::is_same_v<typename TargetTable::Entity, Target>, "ref target table entity does not match Ref<T>");
                static_assert(std::is_same_v<typename Field::Key, typename TargetTable::PK>, "ref key type does not match target table primary key");

                const std::string_view fieldName = binpack::detail::memberName<FieldPtr>();
                for (auto& refField : refFields_) {
                    if (refField.fieldName == fieldName) {
                        auto binding = std::make_unique<TableRefBinding<TargetPrimaryKeyPtr>>(&target);
                        refField.binding = binding.get();
                        refBindings_.push_back(std::move(binding));
                        return *this;
                    }
                }

                auto binding = std::make_unique<TableRefBinding<TargetPrimaryKeyPtr>>(&target);
                auto* rawBinding = binding.get();
                refBindings_.push_back(std::move(binding));
                refFields_.push_back(
                    RefFieldDef{
                        std::string(fieldName),
                        rawBinding,
                        [](const Entity& entity, void* raw) {
                            auto* binding = static_cast<TableRefBinding<TargetPrimaryKeyPtr>*>(raw);
                            (entity.*FieldPtr).attach(binding);
                        },
                        [](const Entity& entity, void* raw) {
                            auto* binding = static_cast<TableRefBinding<TargetPrimaryKeyPtr>*>(raw);
                            const auto& ref = entity.*FieldPtr;
                            ref.attach(binding);
                            if (ref.dirty()) {
                                binding->put(ref.value());
                                ref.markClean();
                            }
                        }
                    }
                );
                return *this;
            }

            template <auto FieldPtr>
            [[nodiscard]] Index<FieldPtr> index() {
                static_assert(std::is_same_v<binpack::detail::classOf<FieldPtr>, Entity>, "index field must belong to the table entity");
                const std::string_view fieldName = binpack::detail::memberName<FieldPtr>();
                const auto prefix = makeIndexPrefix(tableName_, fieldName);

                for (const auto& idx : indexes_) { if (idx.fieldName == fieldName) { return Index<FieldPtr>{this, prefix}; } }

                indexes_.push_back(
                    IndexDef{
                        prefix,
                        std::string(fieldName),
                        [](const Entity& entity, ArenaByteBuffer& out) {
                            out.clear();
                            encodeIndexFieldValue(entity.*FieldPtr, out);
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

            PackedTable& bindRefsFrom(const RefBindingLookup& lookup) {
                refBindingLookup_ = &lookup;
                return *this;
            }

            template <auto FieldPtr>
            PackedTable& foreignKey() {
                static_assert(std::is_same_v<binpack::detail::classOf<FieldPtr>, Entity>, "foreign key field must belong to the table entity");
                using Field = binpack::detail::memberOf<FieldPtr>;
                static_assert(isRef<Field>, "foreignKey field must be akkaradb::Ref<T>");

                const std::string_view fieldName = binpack::detail::memberName<FieldPtr>();
                for (const auto& fk : foreignKeys_) { if (fk.fieldName == fieldName) { return *this; } }

                foreignKeys_.push_back(
                    ForeignKeyDef{
                        std::string(fieldName),
                        [](const Entity& entity, const PackedTable& table) {
                            using Target = typename RefTarget<Field>::Type;
                            const auto* binding = table.template findRefBinding<Target>();
                            if (binding == nullptr) { throw std::runtime_error("AkkaraDB foreign key: Ref target table is not registered"); }

                            const auto& ref = entity.*FieldPtr;
                            if (!binding->exists(ref.id())) {
                                throw std::runtime_error("AkkaraDB foreign key: referenced entity was not found");
                            }
                        }
                    }
                );
                return *this;
            }

            template <auto RefFieldPtr, auto SourcePrimaryKeyPtr>
            PackedTable& cascadeDeleteFrom(PackedTable<SourcePrimaryKeyPtr>& source) {
                using SourceTable = PackedTable<SourcePrimaryKeyPtr>;
                using SourceEntity = typename SourceTable::Entity;
                using Field = binpack::detail::memberOf<RefFieldPtr>;
                static_assert(std::is_same_v<binpack::detail::classOf<RefFieldPtr>, SourceEntity>, "cascade ref field must belong to the source entity");
                static_assert(isRef<Field>, "cascade ref field must be akkaradb::Ref<T>");
                using Target = typename RefTarget<Field>::Type;
                static_assert(std::is_same_v<Target, Entity>, "cascade target table entity does not match Ref<T>");
                static_assert(std::is_same_v<typename Field::Key, PK>, "cascade ref key type does not match target primary key");

                const std::string_view fieldName = binpack::detail::memberName<RefFieldPtr>();
                for (const auto& cascade : cascadeDeletes_) {
                    if (cascade.source == &source && cascade.fieldName == fieldName) { return *this; }
                }

                cascadeDeletes_.push_back(
                    CascadeDeleteDef{
                        &source,
                        std::string(fieldName),
                        [](const PK& targetPk, void* rawSource) {
                            auto* sourceTable = static_cast<SourceTable*>(rawSource);
                            std::vector<typename SourceTable::PK> removeKeys;
                            auto rows = sourceTable->scanAll();
                            while (rows.hasNext()) {
                                auto entry = rows.next();
                                if ((entry.value.*RefFieldPtr).id() == targetPk) { removeKeys.push_back(entry.id); }
                            }
                            for (const auto& key : removeKeys) { sourceTable->remove(key); }
                        }
                    }
                );
                return *this;
            }

            void put(const Entity& entity) {
                resetTempBuffers();
                attachRefBindings(entity);
                flushDirtyRefs(entity);
                validateForeignKeys(entity);
                const PK& pk = entity.*PrimaryKeyPtr;
                makePkKey(pk, pkKeyBuffer_);

                if (!indexes_.empty()) {
                    std::span<const uint8_t> oldBytes;
                    if (engine_->getIntoArena(pkKeyBuffer_, *tempArena_, oldBytes)) {
                        const Entity oldEntity = binpack::BinPack::decode<Entity>(oldBytes);
                        removeIndexEntries(oldEntity, pkKeyBuffer_);
                    }
                }

                valueBuffer_.clear();
                valueBuffer_.reserve(binpack::BinPack::estimateSize(entity));
                binpack::BinPack::encodeInto(entity, valueBuffer_);

                putHinted(pkKeyBuffer_, valueBuffer_);
                if (!indexes_.empty()) { writeIndexEntries(entity, pkKeyBuffer_); }
            }

            [[nodiscard]] std::optional<Entity> get(const PK& pk) const {
                Entity out{};
                if (!getInto(pk, out)) { return std::nullopt; }
                return out;
            }

            [[nodiscard]] bool getInto(const PK& pk, Entity& out) const {
                resetTempBuffers();
                makePkKey(pk, pkKeyBuffer_);
                std::span<const uint8_t> bytes;
                if (!engine_->getIntoArena(pkKeyBuffer_, *tempArena_, bytes)) { return false; }
                const bool decoded = binpack::BinPack::decodeInto<Entity>(bytes, out);
                if (decoded) { attachRefBindings(out); }
                return decoded;
            }

            void remove(const PK& pk) {
                resetTempBuffers();
                runCascadeDeletes(pk);
                makePkKey(pk, pkKeyBuffer_);

                if (!indexes_.empty()) {
                    std::span<const uint8_t> oldBytes;
                    if (engine_->getIntoArena(pkKeyBuffer_, *tempArena_, oldBytes)) {
                        const Entity oldEntity = binpack::BinPack::decode<Entity>(oldBytes);
                        removeIndexEntries(oldEntity, pkKeyBuffer_);
                    }
                }

                removeHinted(pkKeyBuffer_);
            }

            [[nodiscard]] bool exists(const PK& pk) const {
                resetTempBuffers();
                makePkKey(pk, pkKeyBuffer_);
                return engine_->exists(pkKeyBuffer_);
            }

            void upsert(const PK& pk, std::function<void(Entity&)> update) {
                Entity entity = get(pk).value_or(Entity{});
                entity.*PrimaryKeyPtr = pk;
                update(entity);
                put(entity);
            }

            template <auto FieldPtr>
            [[nodiscard]] std::optional<Entity> findBy(const binpack::detail::memberOf<FieldPtr>& value) const {
                static_assert(std::is_same_v<binpack::detail::classOf<FieldPtr>, Entity>, "findBy field must belong to the table entity");
                const std::string_view fieldName = binpack::detail::memberName<FieldPtr>();
                const auto prefix = makeIndexPrefix(tableName_, fieldName);

                bool registered = false;
                for (const auto& idx : indexes_) {
                    if (idx.fieldName == fieldName) {
                        registered = true;
                        break;
                    }
                }
                if (!registered) { throw std::runtime_error("PackedTable::findBy: index is not registered for this field"); }

                resetTempBuffers();
                fieldBuffer_.clear();
                encodeIndexFieldValue(value, fieldBuffer_);
                makeIndexSearchPrefix(prefix, fieldBuffer_, scanStartBuffer_);
                scanEndBuffer_ = scanStartBuffer_;
                if (!detail::incrementLexicographicBytes(scanEndBuffer_.data(), scanEndBuffer_.size())) { scanEndBuffer_.clear(); }

                core::BufferArena scanArena;
                auto rows = engine_->scan(scanArena, scanStartBuffer_, scanEndBuffer_);
                for (auto it = rows.begin(); !(it == rows.end()); ++it) {
                    const auto& raw = *it;
                    const auto key = raw.key;
                    if (key.size() <= scanStartBuffer_.size()) { continue; }

                    Entry entry;
                    const std::span<const uint8_t> pkBytes{key.data() + scanStartBuffer_.size(), key.size() - scanStartBuffer_.size()};
                    if (getByPkBytes(pkBytes, entry)) { return entry.value; }
                }
                return std::nullopt;
            }

            [[nodiscard]] size_t count() const {
                resetTempBuffers();
                makePrefixStartEnd(pkPrefix_, scanStartBuffer_, scanEndBuffer_);
                return engine_->count(scanStartBuffer_, scanEndBuffer_);
            }

            class ScanRange {
                public:
                    ScanRange(ScanRange&&) noexcept = default;
                    ScanRange& operator=(ScanRange&&) noexcept = default;
                    ScanRange(const ScanRange&) = delete;
                    ScanRange& operator=(const ScanRange&) = delete;

                    [[nodiscard]] bool hasNext() const noexcept { return pending_.has_value(); }

                    [[nodiscard]] Entry next() {
                        if (!pending_) { throw std::out_of_range("PackedTable::ScanRange: no next entry"); }
                        Entry out = std::move(*pending_);
                        advance();
                        return out;
                    }

                private:
                    friend class PackedTable;

                    ScanRange(const PackedTable* table, std::span<const uint8_t> startKey, std::span<const uint8_t> endKey)
                        : table_{table},
                          scanArena_{std::make_unique<core::BufferArena>()},
                          rows_{table_->engine_->scan(*scanArena_, startKey, endKey)},
                          it_{rows_.begin()} { advance(); }

                    void advance() {
                        pending_.reset();
                        while (!(it_ == rows_.end())) {
                            const auto& raw = *it_;
                            const auto key = raw.key;
                            if (key.size() < table_->pkPrefix_.size() || std::memcmp(
                                key.data(),
                                table_->pkPrefix_.data(),
                                table_->pkPrefix_.size()
                            ) != 0) { return; }

                            std::span<const uint8_t> pkBytes{key.data() + table_->pkPrefix_.size(), key.size() - table_->pkPrefix_.size()};
                            Entry entry{table_->decodePrimaryKeyBytes(pkBytes), binpack::BinPack::decode<Entity>(raw.value)};
                            table_->attachRefBindings(entry.value);
                            pending_ = std::move(entry);
                            ++it_;
                            return;
                        }
                    }

                    const PackedTable* table_;
                    std::unique_ptr<core::BufferArena> scanArena_;
                    core::ArenaGenerator<engine::AkkEngine::ScanRecordView> rows_;
                    core::ArenaGenerator<engine::AkkEngine::ScanRecordView>::iterator it_;
                    std::optional<Entry> pending_;
            };

            [[nodiscard]] ScanRange scanAll() const {
                resetTempBuffers();
                makePrefixStartEnd(pkPrefix_, scanStartBuffer_, scanEndBuffer_);
                return ScanRange{this, scanStartBuffer_, scanEndBuffer_};
            }

            [[nodiscard]] ScanRange scan(const PK& startPk) const {
                resetTempBuffers();
                makePkKey(startPk, scanStartBuffer_);
                scanEndBuffer_.assign(pkPrefix_.begin(), pkPrefix_.end());
                if (!detail::incrementLexicographicBytes(scanEndBuffer_.data(), scanEndBuffer_.size())) { scanEndBuffer_.clear(); }
                return ScanRange{this, scanStartBuffer_, scanEndBuffer_};
            }

            [[nodiscard]] ScanRange scan(const PK& startPk, const PK& endPk) const {
                resetTempBuffers();
                makePkKey(startPk, scanStartBuffer_);
                makePkKey(endPk, scanEndBuffer_);
                return ScanRange{this, scanStartBuffer_, scanEndBuffer_};
            }

            enum class QuerySourceKind {
                TABLE, INDEX
            };

            struct QueryRange {
                std::vector<uint8_t> startKey;
                std::vector<uint8_t> endKey;
                size_t indexSearchPrefixSize = 0;
                bool dynamicIndexPkOffset = false;
                bool dedupeIndexPks = false;
            };

            struct QueryPlan {
                QuerySourceKind kind = QuerySourceKind::TABLE;
                std::vector<QueryRange> ranges;
            };

            class QuerySource {
                public:
                    QuerySource(QuerySource&&) noexcept = default;
                    QuerySource& operator=(QuerySource&&) noexcept = default;
                    QuerySource(const QuerySource&) = delete;
                    QuerySource& operator=(const QuerySource&) = delete;

                    [[nodiscard]] bool hasNext() const noexcept { return pending_.has_value(); }

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
                          scanArena_{std::make_unique<core::BufferArena>()} {
                        openNextRange();
                        advance();
                    }

                    void openNextRange() {
                        while (rangeIndex_ < ranges_.size()) {
                            const auto& range = ranges_[rangeIndex_++];
                            indexSearchPrefixSize_ = range.indexSearchPrefixSize;
                            dynamicIndexPkOffset_ = range.dynamicIndexPkOffset;
                            dedupeIndexPks_ = range.dedupeIndexPks;
                            it_ = {};
                            rows_ = {};
                            scanArena_->reset();
                            rows_ = table_->engine_->scan(*scanArena_, range.startKey, range.endKey);
                            it_ = rows_.begin();
                            return;
                        }
                    }

                    void advance() {
                        pending_.reset();
                        while (rangeIndex_ <= ranges_.size()) {
                            while (!(it_ == rows_.end())) {
                                const auto& raw = *it_;
                                Entry entry;
                                if (kind_ == QuerySourceKind::TABLE) {
                                    const auto key = raw.key;
                                    if (key.size() < table_->pkPrefix_.size() || std::memcmp(
                                        key.data(),
                                        table_->pkPrefix_.data(),
                                        table_->pkPrefix_.size()
                                    ) != 0) { return; }

                                    std::span<const uint8_t> pkBytes{
                                        key.data() + table_->pkPrefix_.size(),
                                        key.size() - table_->pkPrefix_.size()
                                    };
                                    entry = Entry{table_->decodePrimaryKeyBytes(pkBytes), binpack::BinPack::decode<Entity>(raw.value)};
                                    table_->attachRefBindings(entry.value);
                                }
                                else {
                                    const auto key = raw.key;
                                    size_t pkOffset = indexSearchPrefixSize_;
                                    if (dynamicIndexPkOffset_) {
                                        if (key.size() <= 12) {
                                            ++it_;
                                            continue;
                                        }
                                        size_t fieldSize = readLe32(key.data() + 8);
                                        if (12 + fieldSize >= key.size()) {
                                            const size_t legacyFieldSize = readBe32(key.data() + 8);
                                            if (12 + legacyFieldSize < key.size()) { fieldSize = legacyFieldSize; }
                                        }
                                        pkOffset = 12 + fieldSize;
                                    }
                                    if (key.size() <= pkOffset) {
                                        ++it_;
                                        continue;
                                    }
                                    const std::span<const uint8_t> pkBytes{key.data() + pkOffset, key.size() - pkOffset};
                                    if (dedupeIndexPks_) {
                                        std::string pkKey{reinterpret_cast<const char*>(pkBytes.data()), pkBytes.size()};
                                        if (!seenIndexPks_.insert(std::move(pkKey)).second) {
                                            ++it_;
                                            continue;
                                        }
                                    }
                                    if (!table_->getByPkBytes(pkBytes, entry)) {
                                        ++it_;
                                        continue;
                                    }
                                }
                                pending_ = std::move(entry);
                                ++it_;
                                return;
                            }
                            if (rangeIndex_ >= ranges_.size()) { return; }
                            openNextRange();
                        }
                    }

                    const PackedTable* table_;
                    QuerySourceKind kind_;
                    size_t indexSearchPrefixSize_;
                    bool dynamicIndexPkOffset_ = false;
                    bool dedupeIndexPks_ = false;
                    std::unordered_set<std::string> seenIndexPks_;
                    std::vector<QueryRange> ranges_;
                    size_t rangeIndex_ = 0;
                    std::unique_ptr<core::BufferArena> scanArena_;
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
                                while (source_.hasNext()) {
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
                        table_->makeQueryPlan(expr_, plan);
                        return Iterator{QuerySource{table_, std::move(plan)}, expr_, limit_};
                    }

                    [[nodiscard]] typename Iterator::Sentinel end() const noexcept { return {}; }

                    template <typename Pred>
                    [[nodiscard]] auto where(Pred&& predicate) const {
                        auto next = std::forward<Pred>(predicate)(query::makeProxy<Entity>());
                        using NextExpr = decltype(next);
                        return QueryView<query::Logical<query::Op::AND, Expr, NextExpr>>{
                            table_,
                            query::Logical<query::Op::AND, Expr, NextExpr>{expr_, std::move(next)},
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

                    [[nodiscard]] std::vector<Entry> toVector() const {
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

            template <auto LeftFieldPtr, auto RightFieldPtr, auto TargetPrimaryKeyPtr>
            class JoinView {
                public:
                    using LeftEntry = Entry;
                    using RightTable = PackedTable<TargetPrimaryKeyPtr>;
                    using RightEntry = typename RightTable::Entry;
                    using RightEntity = typename RightTable::Entity;
                    using RightPK = typename RightTable::PK;

                    struct JoinRow {
                        LeftEntry left;
                        RightEntity right;
                    };

                    class Iterator {
                        public:
                            struct Sentinel {};

                            using value_type = JoinRow;
                            using difference_type = std::ptrdiff_t;
                            using iterator_category = std::input_iterator_tag;

                            [[nodiscard]] const JoinRow& operator*() const noexcept { return *current_; }
                            [[nodiscard]] const JoinRow* operator->() const noexcept { return &*current_; }

                            Iterator& operator++() {
                                advance();
                                return *this;
                            }

                            [[nodiscard]] bool operator!=(const Sentinel&) const noexcept { return current_.has_value(); }
                            [[nodiscard]] bool operator==(const Sentinel&) const noexcept { return !current_.has_value(); }

                        private:
                            friend class JoinView;

                            explicit Iterator(const JoinView* view)
                                : view_{view}, scan_{view_->left_->scanAll()} { advance(); }

                            void advance() {
                                current_.reset();
                                if constexpr (usesRightPrimaryKey()) {
                                    while (scan_.hasNext()) {
                                        auto left = scan_.next();
                                        auto right = view_->right_->get(view_->joinKey(left.value.*LeftFieldPtr));
                                        if (!right) { continue; }
                                        if (!view_->predicate_(left.value, *right)) { continue; }
                                        current_ = JoinRow{std::move(left), std::move(*right)};
                                        return;
                                    }
                                }
                                else {
                                    while (true) {
                                        if (!left_) {
                                            if (!scan_.hasNext()) { return; }
                                            left_ = scan_.next();
                                            rightScan_.emplace(view_->right_->scanAll());
                                        }

                                        while (rightScan_->hasNext()) {
                                            auto right = rightScan_->next();
                                            if (!view_->joinFieldsEqual(left_->value, right.value)) { continue; }
                                            if (!view_->predicate_(left_->value, right.value)) { continue; }
                                            current_ = JoinRow{*left_, std::move(right.value)};
                                            return;
                                        }

                                        left_.reset();
                                        rightScan_.reset();
                                    }
                                }
                            }

                            const JoinView* view_;
                            ScanRange scan_;
                            std::optional<LeftEntry> left_;
                            std::optional<typename RightTable::ScanRange> rightScan_;
                            std::optional<JoinRow> current_;
                    };

                    [[nodiscard]] Iterator begin() const { return Iterator{this}; }
                    [[nodiscard]] typename Iterator::Sentinel end() const noexcept { return {}; }

                    template <typename Pred>
                    [[nodiscard]] JoinView where(Pred&& predicate) const {
                        auto previous = predicate_;
                        auto next = std::function<bool(const Entity&, const RightEntity&)>{
                            [previous = std::move(previous), predicate = std::forward<Pred>(predicate)](
                                const Entity& left,
                                const RightEntity& right
                            ) mutable {
                                return previous(left, right) && predicate(left, right);
                            }
                        };
                        return JoinView{left_, right_, std::move(next)};
                    }

                    [[nodiscard]] std::optional<JoinRow> first() const {
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

                    [[nodiscard]] std::vector<JoinRow> toVector() const {
                        std::vector<JoinRow> out;
                        for (const auto& row : *this) { out.push_back(row); }
                        return out;
                    }

                private:
                    friend class PackedTable;

                    JoinView(
                        const PackedTable* left,
                        const RightTable* right,
                        std::function<bool(const Entity&, const RightEntity&)> predicate = [](const Entity&, const RightEntity&) { return true; }
                    )
                        : left_{left}, right_{right}, predicate_{std::move(predicate)} {}

                    template <typename X>
                    static decltype(auto) joinKey(const X& value) {
                        using Field = std::remove_cvref_t<X>;
                        if constexpr (isRef<Field>) {
                            return value.id();
                        }
                        else {
                            return (value);
                        }
                    }

                    static consteval bool usesRightPrimaryKey() {
                        if constexpr (std::is_same_v<decltype(RightFieldPtr), decltype(TargetPrimaryKeyPtr)>) {
                            return RightFieldPtr == TargetPrimaryKeyPtr;
                        }
                        else {
                            return false;
                        }
                    }

                    [[nodiscard]] bool joinFieldsEqual(const Entity& left, const RightEntity& right) const {
                        return joinKey(left.*LeftFieldPtr) == joinKey(right.*RightFieldPtr);
                    }

                    const PackedTable* left_;
                    const RightTable* right_;
                    std::function<bool(const Entity&, const RightEntity&)> predicate_;
            };

            template <auto LeftFieldPtr, auto RightFieldPtr, auto TargetPrimaryKeyPtr>
            [[nodiscard]] JoinView<LeftFieldPtr, RightFieldPtr, TargetPrimaryKeyPtr> join(const PackedTable<TargetPrimaryKeyPtr>& target) const {
                static_assert(std::is_same_v<binpack::detail::classOf<LeftFieldPtr>, Entity>, "join left field must belong to the left table entity");
                using TargetTable = PackedTable<TargetPrimaryKeyPtr>;
                using RightEntity = typename TargetTable::Entity;
                static_assert(std::is_same_v<binpack::detail::classOf<RightFieldPtr>, RightEntity>, "join right field must belong to the right table entity");
                using LeftField = binpack::detail::memberOf<LeftFieldPtr>;
                using RightField = binpack::detail::memberOf<RightFieldPtr>;
                static_assert(
                    requires(const LeftField& left, const RightField& right) {
                        { JoinView<LeftFieldPtr, RightFieldPtr, TargetPrimaryKeyPtr>::joinKey(left)
                            == JoinView<LeftFieldPtr, RightFieldPtr, TargetPrimaryKeyPtr>::joinKey(right) } -> std::convertible_to<bool>;
                    },
                    "join fields must be comparable"
                );
                return JoinView<LeftFieldPtr, RightFieldPtr, TargetPrimaryKeyPtr>{this, &target};
            }

            template <auto RefFieldPtr, auto TargetPrimaryKeyPtr>
                requires(isRef<binpack::detail::memberOf<RefFieldPtr>>)
            [[nodiscard]] auto join(const PackedTable<TargetPrimaryKeyPtr>& target) const {
                static_assert(std::is_same_v<binpack::detail::classOf<RefFieldPtr>, Entity>, "join ref field must belong to the table entity");
                using Field = binpack::detail::memberOf<RefFieldPtr>;
                using Target = typename RefTarget<Field>::Type;
                using TargetTable = PackedTable<TargetPrimaryKeyPtr>;
                static_assert(std::is_same_v<typename TargetTable::Entity, Target>, "join target table entity does not match Ref<T>");
                static_assert(std::is_same_v<typename Field::Key, typename TargetTable::PK>, "join key type does not match target table primary key");
                return join<RefFieldPtr, TargetPrimaryKeyPtr, TargetPrimaryKeyPtr>(target);
            }

            [[nodiscard]] std::string_view tableName() const noexcept { return tableName_; }
            [[nodiscard]] engine::AkkEngine& engine() noexcept { return *engine_; }
            [[nodiscard]] const engine::AkkEngine& engine() const noexcept { return *engine_; }

            template <auto FieldPtr>
            class Index {
                public:
                    using Field = binpack::detail::memberOf<FieldPtr>;

                    class FindRange {
                        public:
                            FindRange(FindRange&&) noexcept = default;
                            FindRange& operator=(FindRange&&) noexcept = default;
                            FindRange(const FindRange&) = delete;
                            FindRange& operator=(const FindRange&) = delete;

                            [[nodiscard]] bool hasNext() const noexcept { return pending_.has_value(); }

                            [[nodiscard]] Entry next() {
                                if (!pending_) { throw std::out_of_range("PackedTable::Index::FindRange: no next entry"); }
                                Entry out = std::move(*pending_);
                                advance();
                                return out;
                            }

                        private:
                            friend class Index;

                            FindRange(
                                PackedTable* table,
                                size_t searchPrefixSize,
                                std::span<const uint8_t> startKey,
                                std::span<const uint8_t> endKey
                            )
                                : table_{table},
                                  searchPrefixSize_{searchPrefixSize},
                                  scanArena_{std::make_unique<core::BufferArena>()},
                                  rows_{table_->engine_->scan(*scanArena_, startKey, endKey)},
                                  it_{rows_.begin()} { advance(); }

                            void advance() {
                                pending_.reset();
                                while (!(it_ == rows_.end())) {
                                    const auto& raw = *it_;
                                    const auto key = raw.key;
                                    if (key.size() <= searchPrefixSize_) {
                                        ++it_;
                                        continue;
                                    }

                                    std::span pkBytes{key.data() + searchPrefixSize_, key.size() - searchPrefixSize_};
                                    Entry entry;
                                    if (table_->getByPkBytes(pkBytes, entry)) {
                                        pending_ = std::move(entry);
                                        ++it_;
                                        return;
                                    }
                                    ++it_;
                                }
                            }

                            PackedTable* table_;
                            size_t searchPrefixSize_;
                            std::unique_ptr<core::BufferArena> scanArena_;
                            core::ArenaGenerator<engine::AkkEngine::ScanRecordView> rows_;
                            core::ArenaGenerator<engine::AkkEngine::ScanRecordView>::iterator it_;
                            std::optional<Entry> pending_;
                    };

                    [[nodiscard]] FindRange find(const Field& value) const {
                        table_->resetTempBuffers();
                        table_->fieldBuffer_.clear();
                        table_->encodeIndexFieldValue(value, table_->fieldBuffer_);
                        table_->makeIndexSearchPrefix(prefix_, table_->fieldBuffer_, table_->scanStartBuffer_);
                        table_->scanEndBuffer_ = table_->scanStartBuffer_;
                        if (!detail::incrementLexicographicBytes(table_->scanEndBuffer_.data(), table_->scanEndBuffer_.size())) {
                            table_->scanEndBuffer_.clear();
                        }
                        return FindRange{table_, table_->scanStartBuffer_.size(), table_->scanStartBuffer_, table_->scanEndBuffer_};
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
                std::string fieldName;
                void (*encodeField)(const Entity&, ArenaByteBuffer&);
            };

            struct RefFieldDef {
                std::string fieldName;
                void* binding;
                void (*attach)(const Entity&, void*);
                void (*flush)(const Entity&, void*);
            };

            struct ForeignKeyDef {
                std::string fieldName;
                void (*validate)(const Entity&, const PackedTable&);
            };

            struct CascadeDeleteDef {
                void* source;
                std::string fieldName;
                void (*cascade)(const PK&, void*);
            };

            template <auto TargetPrimaryKeyPtr>
            class TableRefBinding final : public RefBinding<binpack::detail::classOf<TargetPrimaryKeyPtr>> {
                public:
                    using TargetEntity = binpack::detail::classOf<TargetPrimaryKeyPtr>;
                    using Key = binpack::detail::memberOf<TargetPrimaryKeyPtr>;

                    explicit TableRefBinding(PackedTable<TargetPrimaryKeyPtr>* table) : table_{table} {}

                    [[nodiscard]] bool exists(const Key& key) const override { return table_->exists(key); }
                    [[nodiscard]] std::optional<TargetEntity> get(const Key& key) const override { return table_->get(key); }
                    void put(const TargetEntity& value) override { table_->put(value); }

                private:
                    PackedTable<TargetPrimaryKeyPtr>* table_;
            };

            engine::AkkEngine* engine_ = nullptr;
            std::string tableName_;
            std::array<uint8_t, 8> pkPrefix_{};
            std::vector<IndexDef> indexes_;
            std::vector<RefFieldDef> refFields_;
            std::vector<ForeignKeyDef> foreignKeys_;
            std::vector<CascadeDeleteDef> cascadeDeletes_;
            std::vector<std::unique_ptr<RefBindingBase>> refBindings_;
            const RefBindingLookup* refBindingLookup_ = nullptr;

            mutable std::unique_ptr<core::BufferArena> tempArena_ = std::make_unique<core::BufferArena>();
            mutable ArenaByteBuffer pkKeyBuffer_{tempArena_.get()};
            mutable ArenaByteBuffer valueBuffer_{tempArena_.get()};
            mutable ArenaByteBuffer indexKeyBuffer_{tempArena_.get()};
            mutable ArenaByteBuffer fieldBuffer_{tempArena_.get()};
            mutable ArenaByteBuffer scanStartBuffer_{tempArena_.get()};
            mutable ArenaByteBuffer scanEndBuffer_{tempArena_.get()};

            PackedTable() = default;

            void resetTempBuffers() const {
                tempArena_->reset();
                pkKeyBuffer_.bind(tempArena_.get());
                valueBuffer_.bind(tempArena_.get());
                indexKeyBuffer_.bind(tempArena_.get());
                fieldBuffer_.bind(tempArena_.get());
                scanStartBuffer_.bind(tempArena_.get());
                scanEndBuffer_.bind(tempArena_.get());
            }

            template <typename Expr>
            void makeQueryPlan(const Expr& expr, QueryPlan& plan) const {
                resetTempBuffers();
                if (tryMakeIndexPlan(expr, plan)) { return; }

                plan.kind = QuerySourceKind::TABLE;
                plan.ranges.clear();
                makePrefixStartEnd(pkPrefix_, scanStartBuffer_, scanEndBuffer_);
                addQueryRange(plan, scanStartBuffer_, scanEndBuffer_, 0);
            }

            void attachRefBindings(const Entity& entity) const {
                if (refFields_.empty() && refBindingLookup_ == nullptr) { return; }
                for (const auto& refField : refFields_) { refField.attach(entity, refField.binding); }
                if (refBindingLookup_ != nullptr) { attachAutoRefs(entity); }
            }

            void flushDirtyRefs(const Entity& entity) const {
                if (refFields_.empty() && refBindingLookup_ == nullptr) { return; }
                for (const auto& refField : refFields_) { refField.flush(entity, refField.binding); }
                if (refBindingLookup_ != nullptr) { flushAutoRefs(entity); }
            }

            void validateForeignKeys(const Entity& entity) const {
                for (const auto& fk : foreignKeys_) { fk.validate(entity, *this); }
            }

            void runCascadeDeletes(const PK& pk) {
                for (const auto& cascade : cascadeDeletes_) { cascade.cascade(pk, cascade.source); }
            }

            template <typename Target>
            [[nodiscard]] const RefBinding<Target>* findRefBinding() const {
                if (refBindingLookup_ == nullptr) { return nullptr; }
                auto* raw = refBindingLookup_->findRefBinding(std::type_index(typeid(Target)));
                if (raw == nullptr) { return nullptr; }
                return static_cast<RefBinding<Target>*>(raw);
            }

            template <typename X>
            void attachAutoRefs(const X& value) const {
                using Field = std::remove_cvref_t<X>;
                if constexpr (isRef<Field>) {
                    using Target = typename RefTarget<Field>::Type;
                    auto* raw = refBindingLookup_->findRefBinding(std::type_index(typeid(Target)));
                    if (raw == nullptr) { throw std::runtime_error("AkkaraDB schema: Ref target table is not registered"); }
                    value.attach(static_cast<RefBinding<Target>*>(raw));
                }
                else if constexpr (std::is_aggregate_v<Field> && !std::is_array_v<Field>) {
                    boost::pfr::for_each_field(value, [this](const auto& field) { attachAutoRefs(field); });
                }
            }

            template <typename X>
            void flushAutoRefs(const X& value) const {
                using Field = std::remove_cvref_t<X>;
                if constexpr (isRef<Field>) {
                    using Target = typename RefTarget<Field>::Type;
                    auto* raw = refBindingLookup_->findRefBinding(std::type_index(typeid(Target)));
                    if (raw == nullptr) { throw std::runtime_error("AkkaraDB schema: Ref target table is not registered"); }
                    auto* binding = static_cast<RefBinding<Target>*>(raw);
                    value.attach(binding);
                    if (value.dirty()) {
                        binding->put(value.value());
                        value.markClean();
                    }
                }
                else if constexpr (std::is_aggregate_v<Field> && !std::is_array_v<Field>) {
                    boost::pfr::for_each_field(value, [this](const auto& field) { flushAutoRefs(field); });
                }
            }

            void addQueryRange(
                QueryPlan& plan,
                std::span<const uint8_t> startKey,
                std::span<const uint8_t> endKey,
                size_t indexSearchPrefixSize,
                bool dynamicIndexPkOffset = false,
                bool dedupeIndexPks = false
            ) const {
                QueryRange range;
                range.startKey.assign(startKey.begin(), startKey.end());
                range.endKey.assign(endKey.begin(), endKey.end());
                range.indexSearchPrefixSize = indexSearchPrefixSize;
                range.dynamicIndexPkOffset = dynamicIndexPkOffset;
                range.dedupeIndexPks = dedupeIndexPks;
                plan.ranges.push_back(std::move(range));
            }

            template <typename Expr>
            [[nodiscard]] bool tryMakeIndexPlan(const Expr& expr, QueryPlan& plan) const {
                using E = std::remove_cvref_t<Expr>;
                if constexpr (query::isAnd<E>) { return tryMakeIndexPlan(expr.lhs, plan) || tryMakeIndexPlan(expr.rhs, plan); }
                else if constexpr (query::isCompare<E>) { return tryMakeCompareIndexPlan(expr, plan); }
                else if constexpr (query::isNot<E>) { return tryMakeNotIndexPlan(expr, plan); }
                else if constexpr (query::isUnary<E>) { return tryMakeUnaryIndexPlan(expr, plan); }
                else { return false; }
            }

            template <query::Op Operator, typename L, typename R>
            [[nodiscard]] bool tryMakeCompareIndexPlan(const query::Compare<Operator, L, R>& expr, QueryPlan& plan) const {
                if constexpr (query::isColumn<L> && query::isLiteral<R>) {
                    return tryMakeFieldIndexPlan<Operator, std::remove_cvref_t<L>::fieldPtr>(query::literalValue(expr.rhs), plan);
                }
                else if constexpr (query::isLiteral<L> && query::isColumn<R>) {
                    return tryMakeFieldIndexPlan<query::swappedCompareOp<Operator>, std::remove_cvref_t<R>::fieldPtr>(
                        query::literalValue(expr.lhs),
                        plan
                    );
                }
                else { return false; }
            }

            template <typename X>
            [[nodiscard]] bool tryMakeNotIndexPlan(const query::Not<X>& expr, QueryPlan& plan) const {
                using E = std::remove_cvref_t<X>;
                if constexpr (query::isCompare<E>) { return tryMakeCompareIndexSourcePlan(expr.x, plan); }
                else if constexpr (query::isUnary<E>) { return tryMakeUnaryIndexPlan(expr.x, plan); }
                else { return false; }
            }

            template <query::Op Operator, typename L, typename R>
            [[nodiscard]] bool tryMakeCompareIndexSourcePlan(const query::Compare<Operator, L, R>&, QueryPlan& plan) const {
                if constexpr (query::isColumn<L> && query::isLiteral<R>) {
                    return tryMakeFieldIndexSourcePlan<std::remove_cvref_t<L>::fieldPtr>(plan);
                }
                else if constexpr (query::isLiteral<L> && query::isColumn<R>) {
                    return tryMakeFieldIndexSourcePlan<std::remove_cvref_t<R>::fieldPtr>(plan);
                }
                else { return false; }
            }

            template <query::Op Operator, typename X>
            [[nodiscard]] bool tryMakeUnaryIndexPlan(const query::Unary<Operator, X>&, QueryPlan& plan) const {
                if constexpr ((Operator == query::Op::IS_NULL || Operator == query::Op::IS_NOT_NULL) && query::isColumn<X>) {
                    return tryMakeNullIndexPlan<Operator, std::remove_cvref_t<X>::fieldPtr>(plan);
                }
                else { return false; }
            }

            template <auto FieldPtr>
            [[nodiscard]] const IndexDef* findIndexDefFor() const {
                const std::string_view fieldName = binpack::detail::memberName<FieldPtr>();
                for (const auto& idx : indexes_) { if (idx.fieldName == fieldName) { return &idx; } }
                return nullptr;
            }

            template <typename Field, typename Lit>
            [[nodiscard]] static bool literalToField(const Lit& literal, Field& out) {
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
                    if (value < static_cast<long double>(std::numeric_limits<Field>::lowest()) || value > static_cast<long double>(
                        std::numeric_limits<Field>::max())) { return false; }
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

            [[nodiscard]] static bool likePatternToPrefix(std::string_view pattern, std::string_view& prefix) {
                const size_t wildcard = pattern.find_first_of("%_");
                if (wildcard == std::string_view::npos) {
                    prefix = pattern;
                    return true;
                }
                if (pattern[wildcard] != '%' || wildcard + 1 != pattern.size()) { return false; }
                prefix = pattern.substr(0, wildcard);
                return true;
            }

            [[nodiscard]] static uint32_t readLe32(const uint8_t* src) noexcept {
                return static_cast<uint32_t>(src[0]) | (static_cast<uint32_t>(src[1]) << 8) | (static_cast<uint32_t>(src[2]) << 16) | (
                    static_cast<uint32_t>(src[3]) << 24);
            }

            [[nodiscard]] static uint32_t readBe32(const uint8_t* src) noexcept {
                return (static_cast<uint32_t>(src[0]) << 24) | (static_cast<uint32_t>(src[1]) << 16) | (static_cast<uint32_t>(src[2]) << 8)
                    | static_cast<uint32_t>(src[3]);
            }

            template <typename Field>
            static void encodeIndexFieldValue(const Field& value, ArenaByteBuffer& out) {
                if constexpr (std::is_integral_v<Field> && !std::is_same_v<Field, bool>) {
                    using Unsigned = std::make_unsigned_t<Field>;
                    Unsigned sortable = static_cast<Unsigned>(value);
                    if constexpr (std::is_signed_v<Field>) { sortable ^= (Unsigned{1} << (sizeof(Field) * 8 - 1)); }
                    writeIndexBigEndian(sortable, out);
                }
                else if constexpr (std::is_same_v<Field, float>) {
                    uint32_t bits;
                    std::memcpy(&bits, &value, sizeof(bits));
                    const uint32_t sign = uint32_t{1} << 31;
                    bits = (bits & sign) != 0 ? ~bits : bits ^ sign;
                    writeIndexBigEndian(bits, out);
                }
                else if constexpr (std::is_same_v<Field, double>) {
                    uint64_t bits;
                    std::memcpy(&bits, &value, sizeof(bits));
                    const uint64_t sign = uint64_t{1} << 63;
                    bits = (bits & sign) != 0 ? ~bits : bits ^ sign;
                    writeIndexBigEndian(bits, out);
                }
                else { binpack::BinPack::encodeInto(value, out); }
            }

            template <typename UInt>
            static void writeIndexBigEndian(UInt value, ArenaByteBuffer& out) {
                static_assert(std::is_unsigned_v<UInt>);
                for (size_t i = sizeof(UInt); i > 0; --i) { out.push_back(static_cast<uint8_t>(value >> ((i - 1) * 8))); }
            }

            [[nodiscard]] bool tryMakeStringPrefixIndexPlan(
                const std::array<uint8_t, 8>& indexPrefix,
                std::string_view prefix,
                QueryPlan& plan
            ) const {
                // String fields are length-prefixed in BinPack, so a content prefix is not a contiguous byte range.
                // Use the field index as the source and let the query predicate apply the residual string filter.
                (void)prefix;
                return tryMakeFullFieldIndexPlan(indexPrefix, plan);
            }

            [[nodiscard]] bool tryMakeFullFieldIndexPlan(const std::array<uint8_t, 8>& indexPrefix, QueryPlan& plan) const {
                makePrefixStartEnd(indexPrefix, scanStartBuffer_, scanEndBuffer_);
                plan.kind = QuerySourceKind::INDEX;
                plan.ranges.clear();
                addQueryRange(plan, scanStartBuffer_, scanEndBuffer_, 0, true, true);
                return true;
            }

            template <auto FieldPtr>
            [[nodiscard]] bool tryMakeFieldIndexSourcePlan(QueryPlan& plan) const {
                const IndexDef* idx = findIndexDefFor<FieldPtr>();
                if (idx == nullptr) { return false; }
                return tryMakeFullFieldIndexPlan(idx->prefix, plan);
            }

            template <typename Field>
            void addEqualityIndexRange(const IndexDef& idx, const Field& value, QueryPlan& plan, bool dedupeIndexPks = false) const {
                fieldBuffer_.clear();
                encodeIndexFieldValue(value, fieldBuffer_);
                makeIndexSearchPrefix(idx.prefix, fieldBuffer_, scanStartBuffer_);
                scanEndBuffer_ = scanStartBuffer_;
                if (!detail::incrementLexicographicBytes(scanEndBuffer_.data(), scanEndBuffer_.size())) { scanEndBuffer_.clear(); }
                addQueryRange(plan, scanStartBuffer_, scanEndBuffer_, scanStartBuffer_.size(), false, dedupeIndexPks);
            }

            template <query::Op Operator, typename Field>
            void addOrderedIndexRange(const IndexDef& idx, const Field& value, QueryPlan& plan) const {
                fieldBuffer_.clear();
                encodeIndexFieldValue(value, fieldBuffer_);

                ArenaByteBuffer boundary{tempArena_.get()};
                makeIndexSearchPrefix(idx.prefix, fieldBuffer_, boundary);
                const size_t pkOffset = boundary.size();

                makePrefixStartEnd(idx.prefix, scanStartBuffer_, scanEndBuffer_);

                if constexpr (Operator == query::Op::GT) {
                    scanStartBuffer_ = boundary;
                    if (!detail::incrementLexicographicBytes(scanStartBuffer_.data(), scanStartBuffer_.size())) {
                        scanStartBuffer_.clear();
                    }
                }
                else if constexpr (Operator == query::Op::GE) { scanStartBuffer_ = boundary; }
                else if constexpr (Operator == query::Op::LT) { scanEndBuffer_ = boundary; }
                else if constexpr (Operator == query::Op::LE) {
                    scanEndBuffer_ = boundary;
                    if (!detail::incrementLexicographicBytes(scanEndBuffer_.data(), scanEndBuffer_.size())) { scanEndBuffer_.clear(); }
                }

                addQueryRange(plan, scanStartBuffer_, scanEndBuffer_, pkOffset);
            }

            template <typename Field>
            static constexpr bool orderedIndexRangeSupportedV = (std::is_arithmetic_v<Field> && !std::is_same_v<Field, bool>);

            template <typename Field, typename Values>
            [[nodiscard]] bool tryMakeInIndexPlan(const IndexDef& idx, const Values& values, QueryPlan& plan) const {
                if constexpr (requires { std::begin(values); std::end(values); }) {
                    plan.kind = QuerySourceKind::INDEX;
                    plan.ranges.clear();
                    for (const auto& literal : values) {
                        Field value{};
                        if (!literalToField<Field>(literal, value)) { continue; }
                        addEqualityIndexRange(idx, value, plan, true);
                    }
                    return true;
                }
                else { return false; }
            }

            template <query::Op Operator, auto FieldPtr>
            [[nodiscard]] bool tryMakeNullIndexPlan(QueryPlan& plan) const {
                using Field = binpack::detail::memberOf<FieldPtr>;
                if constexpr (!query::isOptional<Field>) { return false; }
                else {
                    const IndexDef* idx = findIndexDefFor<FieldPtr>();
                    if (idx == nullptr) { return false; }

                    if constexpr (Operator == query::Op::IS_NULL) {
                        plan.kind = QuerySourceKind::INDEX;
                        plan.ranges.clear();
                        addEqualityIndexRange(*idx, Field{}, plan);
                        return true;
                    }
                    else { return tryMakeFullFieldIndexPlan(idx->prefix, plan); }
                }
            }

            template <query::Op Operator, auto FieldPtr, typename Lit>
            [[nodiscard]] bool tryMakeFieldIndexPlan(const Lit& literal, QueryPlan& plan) const {
                using Field = binpack::detail::memberOf<FieldPtr>;
                const IndexDef* idx = findIndexDefFor<FieldPtr>();
                if (idx == nullptr) { return false; }

                if constexpr (Operator == query::Op::EQ) {
                    Field value{};
                    if (!literalToField<Field>(literal, value)) { return false; }
                    plan.kind = QuerySourceKind::INDEX;
                    plan.ranges.clear();
                    addEqualityIndexRange(*idx, value, plan);
                    return true;
                }
                else if constexpr (Operator == query::Op::NE) {
                    Field value{};
                    if (!literalToField<Field>(literal, value)) { return false; }
                    return tryMakeFullFieldIndexPlan(idx->prefix, plan);
                }
                else if constexpr (Operator == query::Op::IN_LIST) { return tryMakeInIndexPlan<Field>(*idx, literal, plan); }
                else if constexpr (Operator == query::Op::NOT_IN) {
                    if constexpr (requires { std::begin(literal); std::end(literal); }) {
                        return tryMakeFullFieldIndexPlan(idx->prefix, plan);
                    }
                    else { return false; }
                }
                else if constexpr (Operator == query::Op::STARTS_WITH) {
                    if constexpr (query::isStringLikeV<Field> && query::isStringLikeV<Lit>) {
                        return tryMakeStringPrefixIndexPlan(idx->prefix, std::string_view{literal}, plan);
                    }
                    else { return false; }
                }
                else if constexpr (Operator == query::Op::LIKE) {
                    if constexpr (query::isStringLikeV<Field> && query::isStringLikeV<Lit>) {
                        const std::string_view pattern{literal};
                        if (pattern.find_first_of("%_") == std::string_view::npos) {
                            Field value{};
                            if (!literalToField<Field>(pattern, value)) { return false; }
                            plan.kind = QuerySourceKind::INDEX;
                            plan.ranges.clear();
                            addEqualityIndexRange(*idx, value, plan);
                            return true;
                        }
                        std::string_view prefix;
                        if (!likePatternToPrefix(pattern, prefix)) { prefix = {}; }
                        return tryMakeStringPrefixIndexPlan(idx->prefix, prefix, plan);
                    }
                    else { return false; }
                }
                else if constexpr (Operator == query::Op::CONTAINS) {
                    if constexpr (query::isStringLikeV<Field> && query::isStringLikeV<Lit>) {
                        return tryMakeFullFieldIndexPlan(idx->prefix, plan);
                    }
                    else { return false; }
                }
                else if constexpr (Operator == query::Op::GT || Operator == query::Op::GE || Operator == query::Op::LT || Operator ==
                    query::Op::LE) {
                    if constexpr (orderedIndexRangeSupportedV<Field>) {
                        Field value{};
                        if (!literalToField<Field>(literal, value)) { return false; }
                        plan.kind = QuerySourceKind::INDEX;
                        plan.ranges.clear();
                        addOrderedIndexRange<Operator>(*idx, value, plan);
                        return true;
                    }
                    else { return false; }
                }
                else { return false; }
            }

            void makePkKey(const PK& pk, ArenaByteBuffer& out) const {
                out.clear();
                out.insert(out.end(), pkPrefix_.begin(), pkPrefix_.end());
                encodePrimaryKeyBytes(pk, out);
            }

            template <typename Key>
            static void encodePrimaryKeyBytes(const Key& pk, ArenaByteBuffer& out) {
                if constexpr (std::is_integral_v<Key> && !std::is_same_v<Key, bool> && sizeof(Key) <= 8) { encodeSortableIntegral(pk, out); }
                else {
                    binpack::BinPack::encodeInto(pk, out);
                }
            }

            [[nodiscard]] bool getByPkBytes(std::span<const uint8_t> pkBytes, Entry& out) const {
                pkKeyBuffer_.clear();
                pkKeyBuffer_.reserve(8 + pkBytes.size());
                pkKeyBuffer_.insert(pkKeyBuffer_.end(), pkPrefix_.begin(), pkPrefix_.end());
                pkKeyBuffer_.insert(pkKeyBuffer_.end(), pkBytes.begin(), pkBytes.end());

                std::span<const uint8_t> valueSpan;
                if (!engine_->getIntoArena(pkKeyBuffer_, *tempArena_, valueSpan)) { return false; }
                out = Entry{decodePrimaryKeyBytes(pkBytes), binpack::BinPack::decode<Entity>(valueSpan)};
                attachRefBindings(out.value);
                return true;
            }

            [[nodiscard]] static PK decodePrimaryKeyBytes(std::span<const uint8_t> pkBytes) {
                if constexpr (std::is_integral_v<PK> && !std::is_same_v<PK, bool> && sizeof(PK) <= 8) {
                    return decodeSortableIntegral<PK>(pkBytes);
                }
                else { return binpack::BinPack::decode<PK>(pkBytes); }
            }

            template <typename Integral>
            static void encodeSortableIntegral(Integral value, ArenaByteBuffer& out) {
                using Unsigned = std::make_unsigned_t<Integral>;
                Unsigned sortable = static_cast<Unsigned>(value);
                if constexpr (std::is_signed_v<Integral>) { sortable ^= (Unsigned{1} << (sizeof(Integral) * 8 - 1)); }
                writeIndexBigEndian(sortable, out);
            }

            template <typename Integral>
            [[nodiscard]] static Integral decodeSortableIntegral(std::span<const uint8_t> bytes) {
                if (bytes.size() != sizeof(Integral)) { throw std::invalid_argument("PackedTable: malformed primary key"); }

                using Unsigned = std::make_unsigned_t<Integral>;
                Unsigned sortable = 0;
                for (uint8_t b : bytes) { sortable = static_cast<Unsigned>((sortable << 8) | static_cast<Unsigned>(b)); }
                if constexpr (std::is_signed_v<Integral>) { sortable ^= (Unsigned{1} << (sizeof(Integral) * 8 - 1)); }
                return std::bit_cast<Integral>(sortable);
            }

            void putHinted(std::span<const uint8_t> key, std::span<const uint8_t> value) {
                engine_->putHinted(key, value, computeKeyFp64(key), buildMiniKey(key));
            }

            void removeHinted(std::span<const uint8_t> key) { engine_->removeHinted(key, computeKeyFp64(key), buildMiniKey(key)); }

            static void makePrefixStartEnd(const std::array<uint8_t, 8>& prefix, ArenaByteBuffer& start, ArenaByteBuffer& end) {
                start.assign(prefix.begin(), prefix.end());
                end.assign(prefix.begin(), prefix.end());
                if (!detail::incrementLexicographicBytes(end.data(), end.size())) { end.clear(); }
            }

            static std::array<uint8_t, 8> makeTablePrefix(std::string_view name) {
                std::array<uint8_t, 8> out{};
                detail::writeLe64(detail::fnv1a64(name), out.data());
                return out;
            }

            static std::array<uint8_t, 8> makeIndexPrefix(std::string_view tableName, std::string_view fieldName) {
                std::string input;
                input.reserve(tableName.size() + 5 + fieldName.size());
                input.append(tableName);
                input.append(":idx:");
                input.append(fieldName);
                std::array<uint8_t, 8> out{};
                detail::writeLe64(detail::fnv1a64(input), out.data());
                return out;
            }

            void makeIndexSearchPrefix(
                const std::array<uint8_t, 8>& prefix,
                std::span<const uint8_t> fieldBytes,
                ArenaByteBuffer& out
            ) const {
                out.clear();
                out.resize(12 + fieldBytes.size());
                std::memcpy(out.data(), prefix.data(), prefix.size());
                detail::writeLe32(static_cast<uint32_t>(fieldBytes.size()), out.data() + 8);
                if (!fieldBytes.empty()) { std::memcpy(out.data() + 12, fieldBytes.data(), fieldBytes.size()); }
            }

            void makeIndexKey(
                const std::array<uint8_t, 8>& prefix,
                std::span<const uint8_t> fieldBytes,
                std::span<const uint8_t> pkKey,
                ArenaByteBuffer& out
            ) const {
                if (pkKey.size() < pkPrefix_.size()) { throw std::invalid_argument("PackedTable: malformed primary key"); }
                makeIndexSearchPrefix(prefix, fieldBytes, out);
                out.insert(out.end(), pkKey.begin() + static_cast<std::ptrdiff_t>(pkPrefix_.size()), pkKey.end());
            }

            void writeIndexEntries(const Entity& entity, std::span<const uint8_t> pkKey) {
                constexpr std::span<const uint8_t> emptyValue{};
                for (const auto& idx : indexes_) {
                    idx.encodeField(entity, fieldBuffer_);
                    makeIndexKey(idx.prefix, fieldBuffer_, pkKey, indexKeyBuffer_);
                    putHinted(indexKeyBuffer_, emptyValue);
                }
            }

            void removeIndexEntries(const Entity& entity, std::span<const uint8_t> pkKey) {
                for (const auto& idx : indexes_) {
                    idx.encodeField(entity, fieldBuffer_);
                    makeIndexKey(idx.prefix, fieldBuffer_, pkKey, indexKeyBuffer_);
                    removeHinted(indexKeyBuffer_);
                }
            }
    };
} // namespace akkaradb

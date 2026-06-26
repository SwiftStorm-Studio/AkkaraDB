/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkaradb/include/akkaradb/detail/query/Expr.hpp
#pragma once

#include <concepts>
#include <initializer_list>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
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
        using LiteralStorage = std::conditional_t<
            std::is_convertible_v<T, std::string_view> && !std::is_arithmetic_v<std::remove_cvref_t<T>>,
            std::string,
            std::remove_cvref_t<T>
        >;

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
    } // namespace query
} // namespace akkaradb

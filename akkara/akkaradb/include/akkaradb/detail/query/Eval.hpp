/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkaradb/include/akkaradb/detail/query/Eval.hpp
#pragma once

#include "Traits.hpp"

#include <cstddef>
#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>

namespace akkaradb {
    namespace query {
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
            if constexpr (std::is_integral_v<L>&& std::is_integral_v<R>) { return std::cmp_equal(lhs, rhs); }
            else { return lhs == rhs; }
        }

        template <typename L, typename R>
        [[nodiscard]] bool valueLt(const L& lhs, const R& rhs) {
            if constexpr (std::is_integral_v<L>&& std::is_integral_v<R>) { return std::cmp_less(lhs, rhs); }
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
} // namespace akkaradb

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

// akkaradb/include/akkaradb/detail/query/Traits.hpp
#pragma once

#include "Expr.hpp"

#include <optional>
#include <string_view>
#include <type_traits>

namespace akkaradb {
    namespace query {
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
    } // namespace query
} // namespace akkaradb

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkaradb/include/akkaradb/detail/PTImmutable.hpp
#pragma once

template <typename X>
static void sealImmutableFields(const X& value) {
    using Field = std::remove_cvref_t<X>;
    if constexpr (isImmutableField<Field>) { value.seal(); }
    else if constexpr (query::isOptional<Field>) { if (value) { sealImmutableFields(*value); } }
    else if constexpr (std::is_aggregate_v<Field> && !std::is_array_v<Field>) {
        boost::pfr::for_each_field(value, [](const auto& field) { sealImmutableFields(field); });
    }
}

template <typename X>
static void ensureImmutableFieldsUnchanged(const X& oldValue, const X& newValue) {
    using Field = std::remove_cvref_t<X>;
    if constexpr (isImmutableField<Field>) {
        if (!(oldValue.get() == newValue.get())) {
            throw std::runtime_error("AkkaraDB Immutable: persisted field cannot be modified");
        }
    }
    else if constexpr (query::isOptional<Field>) {
        using Inner = std::remove_cvref_t<decltype(*std::declval<const Field&>())>;
        if constexpr (isImmutableField<Inner>) {
            if (oldValue.has_value() != newValue.has_value()) {
                throw std::runtime_error("AkkaraDB Immutable: persisted field cannot be modified");
            }
        }
        if (!oldValue.has_value() || !newValue.has_value()) { return; }
        ensureImmutableFieldsUnchanged(*oldValue, *newValue);
    }
    else if constexpr (std::is_aggregate_v<Field> && !std::is_array_v<Field>) {
        ensureImmutableAggregateFieldsUnchanged(
            oldValue,
            newValue,
            std::make_index_sequence<boost::pfr::tuple_size_v<Field>>{}
        );
    }
}

template <typename X, size_t... I>
static void ensureImmutableAggregateFieldsUnchanged(const X& oldValue, const X& newValue, std::index_sequence<I...>) {
    (ensureImmutableFieldsUnchanged(boost::pfr::get < I > (oldValue), boost::pfr::get < I > (newValue)), ...);
}

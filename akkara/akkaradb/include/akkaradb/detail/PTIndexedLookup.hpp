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

// akkaradb/include/akkaradb/detail/PTIndexedLookup.hpp
#pragma once

template <typename IndexedField, typename Value, typename Out>
static void encodeIndexedSearchFieldValue(const Value& value, Out& out) {
    using Indexed = std::remove_cvref_t<IndexedField>;
    using ValueField = std::remove_cvref_t<Value>;
    if constexpr (query::isOptional<Indexed>) {
        using Inner = typename ForeignKeyValueTraits<Indexed>::Type;
        if constexpr (query::isOptional<ValueField>) { encodeIndexFieldValue(value, out); }
        else if constexpr (isRef<typename std::remove_cvref_t<decltype(*std::declval<Indexed&>())>>) {
            encodeIndexFieldValue(Indexed{typename std::remove_cvref_t<decltype(*std::declval<Indexed&>())>{value}}, out);
        }
        else { encodeIndexFieldValue(Indexed{value}, out); }
    }
    else if constexpr (isRef<Indexed>) {
        if constexpr (std::is_same_v<ValueField, RowId>) { encodeIndexFieldValue(value, out); }
        else if constexpr (!isRef<ValueField>) { encodeIndexFieldValue(Indexed{value}, out); }
        else { encodeIndexFieldValue(value, out); }
    }
    else { encodeIndexFieldValue(value, out); }
}

template <auto FieldPtr, typename Value>
[[nodiscard]] bool hasAnyByIndexedFieldValue(const Value& value) const {
    static_assert(std::is_same_v < binpack::detail::classOf < FieldPtr >,
    Entity >, "indexed field lookup must belong to the table entity"
    )
    ;
    using IndexedField = binpack::detail::memberOf<FieldPtr>;
    std::vector<uint8_t> fieldBytes;
    encodeIndexedSearchFieldValue<IndexedField>(value, fieldBytes);
    std::vector<uint8_t> startKey;
    makeIndexSearchPrefix(makeIndexPrefix(tableName_, binpack::detail::memberName<FieldPtr>()), fieldBytes, startKey);
    std::vector<uint8_t> endKey = startKey;
    if (!detail::incrementLexicographicBytes(endKey.data(), endKey.size())) { endKey.clear(); }

    core::BufferArena scanArena;
    auto rows = engine_->scan(scanArena, startKey, endKey);
    return !(rows.begin() == rows.end());
}

template <auto FieldPtr, typename Value>
void collectPrimaryKeysByIndexedFieldValue(const Value& value, std::vector<PK>& out) const {
    static_assert(std::is_same_v < binpack::detail::classOf < FieldPtr >,
    Entity >, "indexed field lookup must belong to the table entity"
    )
    ;
    using IndexedField = binpack::detail::memberOf<FieldPtr>;
    std::vector<uint8_t> fieldBytes;
    encodeIndexedSearchFieldValue<IndexedField>(value, fieldBytes);
    std::vector<uint8_t> startKey;
    makeIndexSearchPrefix(makeIndexPrefix(tableName_, binpack::detail::memberName<FieldPtr>()), fieldBytes, startKey);
    std::vector<uint8_t> endKey = startKey;
    if (!detail::incrementLexicographicBytes(endKey.data(), endKey.size())) { endKey.clear(); }

    core::BufferArena scanArena;
    auto rows = engine_->scan(scanArena, startKey, endKey);
    for (auto it = rows.begin(); !(it == rows.end()); ++it) {
        const auto& raw = *it;
        const auto key = raw.key;
        if (key.size() <= startKey.size()) { continue; }
        std::span<const uint8_t> pkBytes{key.data() + startKey.size(), key.size() - startKey.size()};
        out.push_back(decodePrimaryKeyBytes(pkBytes));
    }
}

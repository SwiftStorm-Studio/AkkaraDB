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

// akkaradb/include/akkaradb/detail/PTFind.hpp
#pragma once

template <auto FieldPtr>
[[nodiscard]] std::optional<Entity> findBy(const binpack::detail::memberOf<FieldPtr>& value) const {
    static_assert(std::is_same_v < binpack::detail::classOf < FieldPtr >,
    Entity >, "findBy field must belong to the table entity"
    )
    ;
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

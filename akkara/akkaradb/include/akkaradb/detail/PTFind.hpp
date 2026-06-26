/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
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

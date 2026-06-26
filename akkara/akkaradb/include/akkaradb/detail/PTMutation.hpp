/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkaradb/include/akkaradb/detail/PTMutation.hpp
#pragma once

void put(const Entity& entity) {
    resetTempBuffers();
    Entity workingEntity = entity;
    const PK& pk = workingEntity.*PrimaryKeyPtr;
    const auto existingRowId = rowIdOf(pk);
    makePkKey(pk, pkKeyBuffer_);
    std::optional<Entity> oldEntity;

    if (!indexes_.empty() || !updateFieldHooks_.empty()) {
        std::span<const uint8_t> oldBytes;
        if (engine_->getIntoArena(pkKeyBuffer_, *tempArena_, oldBytes)) {
            oldEntity = binpack::BinPack::decode<Entity>(oldBytes);
            attachRefBindings(*oldEntity);
            sealImmutableFields(*oldEntity);
            if (!updateFieldHooks_.empty()) { runUpdateFieldHooks(*oldEntity, workingEntity); }
            ensureImmutableFieldsUnchanged(*oldEntity, workingEntity);
            if (!indexes_.empty()) { removeIndexEntries(*oldEntity, pkKeyBuffer_); }
        }
    }
    else if (existingRowId.has_value()) {
        oldEntity = get(pk);
        if (oldEntity) {
            if (!updateFieldHooks_.empty()) { runUpdateFieldHooks(*oldEntity, workingEntity); }
            ensureImmutableFieldsUnchanged(*oldEntity, workingEntity);
        }
    }

    attachRefBindings(workingEntity);
    flushDirtyRefs(workingEntity);
    validateForeignKeys(workingEntity);
    valueBuffer_.clear();
    valueBuffer_.reserve(binpack::BinPack::estimateSize(workingEntity));
    binpack::BinPack::encodeInto(workingEntity, valueBuffer_);

    putHinted(pkKeyBuffer_, valueBuffer_);
    writeRowIdMapping(pk, existingRowId.value_or(allocateRowId()));
    if (!indexes_.empty()) { writeIndexEntries(workingEntity, pkKeyBuffer_); }
    sealImmutableFields(workingEntity);
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
    if (decoded) {
        attachRefBindings(out);
        sealImmutableFields(out);
    }
    return decoded;
}

[[nodiscard]] std::optional<StableRowId> rowIdOf(const PK& pk) const {
    resetTempBuffers();
    makePkToRowIdKey(pk, pkKeyBuffer_);
    std::span<const uint8_t> bytes;
    if (!engine_->getIntoArena(pkKeyBuffer_, *tempArena_, bytes)) { return std::nullopt; }
    return decodeRowId(bytes);
}

[[nodiscard]] std::optional<PK> primaryKeyOf(StableRowId rowId) const {
    resetTempBuffers();
    makeRowIdToPkKey(rowId, pkKeyBuffer_);
    std::span<const uint8_t> bytes;
    if (!engine_->getIntoArena(pkKeyBuffer_, *tempArena_, bytes)) { return std::nullopt; }
    return decodePrimaryKeyBytes(bytes);
}

[[nodiscard]] std::optional<Entity> getByRowId(StableRowId rowId) const {
    Entity out{};
    if (!getIntoByRowId(rowId, out)) { return std::nullopt; }
    return out;
}

[[nodiscard]] bool getIntoByRowId(StableRowId rowId, Entity& out) const {
    const auto pk = primaryKeyOf(rowId);
    if (!pk) { return false; }
    return getInto(*pk, out);
}

void remove(const PK& pk) {
    resetTempBuffers();
    const auto stableRowId = rowIdOf(pk);
    makePkKey(pk, pkKeyBuffer_);
    std::vector<uint8_t> pkKeyCopy{pkKeyBuffer_.begin(), pkKeyBuffer_.end()};

    std::optional<Entity> oldEntity;
    if (!indexes_.empty() || !cascadeDeletes_.empty() || !restrictDeletes_.empty() || !setNullDeletes_.empty()) {
        std::span<const uint8_t> oldBytes;
        if (engine_->getIntoArena(pkKeyCopy, *tempArena_, oldBytes)) { oldEntity = binpack::BinPack::decode<Entity>(oldBytes); }
    }

    if (oldEntity) { runRestrictDeletes(*oldEntity); }
    if (oldEntity) { runSetNullDeletes(*oldEntity); }
    if (oldEntity) { runCascadeDeletes(*oldEntity); }
    if (oldEntity && !indexes_.empty()) { removeIndexEntries(*oldEntity, pkKeyCopy); }
    removeHinted(pkKeyCopy);
    if (stableRowId) { removeRowIdMapping(pk, *stableRowId); }
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

void updatePrimaryKey(const PK& oldPk, const Entity& entity) {
    const PK& newPk = entity.*PrimaryKeyPtr;
    if (oldPk == newPk) {
        put(entity);
        return;
    }

    resetTempBuffers();
    const auto stableRowId = rowIdOf(oldPk);
    if (!stableRowId) { throw std::runtime_error("PackedTable::updatePrimaryKey: source entity was not found"); }

    auto oldEntity = get(oldPk);
    if (!oldEntity) { throw std::runtime_error("PackedTable::updatePrimaryKey: source entity was not found"); }
    if (exists(newPk)) { throw std::runtime_error("PackedTable::updatePrimaryKey: destination primary key already exists"); }
    Entity workingEntity = entity;
    runUpdateFieldHooks(*oldEntity, workingEntity);
    ensureImmutableFieldsUnchanged(*oldEntity, workingEntity);
    runRestrictUpdates(*oldEntity, workingEntity);

    attachRefBindings(workingEntity);
    flushDirtyRefs(workingEntity);
    validateForeignKeys(workingEntity);

    makePkKey(oldPk, pkKeyBuffer_);
    std::vector<uint8_t> oldPkKeyCopy{pkKeyBuffer_.begin(), pkKeyBuffer_.end()};
    valueBuffer_.clear();
    valueBuffer_.reserve(binpack::BinPack::estimateSize(workingEntity));
    binpack::BinPack::encodeInto(workingEntity, valueBuffer_);

    scanStartBuffer_.clear();
    makePkKey(newPk, scanStartBuffer_);
    putHinted(scanStartBuffer_, valueBuffer_);
    if (!indexes_.empty()) { writeIndexEntries(workingEntity, scanStartBuffer_); }
    rewriteRowIdMapping(oldPk, newPk, *stableRowId);
    if (updateActionsNeedTargetWrite_) {
        runSetNullUpdates(*oldEntity, workingEntity);
        runCascadeUpdates(*oldEntity, workingEntity);
    }
    else {
        runCascadeUpdates(*oldEntity, workingEntity);
        runSetNullUpdates(*oldEntity, workingEntity);
    }
    if (!indexes_.empty()) { removeIndexEntries(*oldEntity, oldPkKeyCopy); }
    removeHinted(oldPkKeyCopy);
    sealImmutableFields(workingEntity);
}

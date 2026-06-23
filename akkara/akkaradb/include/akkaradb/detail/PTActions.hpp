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

// akkaradb/include/akkaradb/detail/PTActions.hpp
#pragma once

template <auto RefFieldPtr, auto SourcePrimaryKeyPtr, auto TargetFieldPtr = PrimaryKeyPtr>
PackedTable& cascadeDeleteFrom(PackedTable<SourcePrimaryKeyPtr>& source) {
    using SourceTable = PackedTable<SourcePrimaryKeyPtr>;
    using SourceEntity = typename SourceTable::Entity;
    using Field = binpack::detail::memberOf<RefFieldPtr>;
    using TargetField = binpack::detail::memberOf<TargetFieldPtr>;
    using ComparableField = ForeignKeyComparableType<Field>;
    using ComparableTargetField = std::conditional_t<isRef<Field>, RowId, ForeignKeyComparableType<TargetField>>;
    static_assert(std::is_same_v < binpack::detail::classOf < RefFieldPtr >,
    SourceEntity >, "cascade ref field must belong to the source entity"
    )
    ;
    static_assert(std::is_same_v < binpack::detail::classOf < TargetFieldPtr >,
    Entity >, "cascade target field must belong to the target entity"
    )
    ;
    static_assert(
        !isRef<Field> || sameMemberPointer<TargetFieldPtr, PrimaryKeyPtr>(),
        "Ref cascade delete currently requires target primary key"
    );
    static_assert(
        requires(const ComparableField& field, const ComparableTargetField& targetField) {
            { field == targetField } -> std::convertible_to<bool>;
        },
        "cascade fields must be comparable"
    );
    static_assert(!isRef<TargetField>, "cascade target field cannot be akkaradb::Ref<T>");

    const std::string_view fieldName = binpack::detail::memberName<RefFieldPtr>();
    for (const auto& cascade : cascadeDeletes_) {
        if (cascade.source == &source && cascade.fieldName == fieldName && cascade.targetFieldName ==
            binpack::detail::memberName<TargetFieldPtr>()) { return *this; }
    }

    (void)source.template index<RefFieldPtr>();
    cascadeDeletes_.push_back(
        CascadeDeleteDef{
            &source,
            this,
            std::string(fieldName),
            std::string(binpack::detail::memberName<TargetFieldPtr>()),
            [](const Entity& targetEntity, void* rawSource, void* rawTarget) {
                auto* sourceTable = static_cast<SourceTable*>(rawSource);
                auto* targetTable = static_cast<PackedTable*>(rawTarget);
                std::vector < typename SourceTable::PK > removeKeys;
                const auto targetValue = [&]() -> ComparableTargetField {
                    if constexpr (isRef<Field>) { return *targetTable->rowIdOf(targetEntity.*TargetFieldPtr); }
                    else { return foreignKeyComparable(targetEntity.*TargetFieldPtr); }
                }();
                auto scan = sourceTable->scanAll();
                while (scan.hasNext()) {
                    auto entry = scan.next();
                    const auto& fieldValue = entry.value.*RefFieldPtr;
                    if (!foreignKeyHasValue(fieldValue)) { continue; }
                    if (foreignKeyComparable(fieldValue) == targetValue) { removeKeys.push_back(entry.id); }
                }
                for (const auto& key : removeKeys) { sourceTable->remove(key); }
            }
        }
    );
    return *this;
}

template <auto RefFieldPtr, auto SourcePrimaryKeyPtr, auto TargetFieldPtr = PrimaryKeyPtr>
PackedTable& restrictDeleteFrom(PackedTable<SourcePrimaryKeyPtr>& source) {
    using SourceTable = PackedTable<SourcePrimaryKeyPtr>;
    using SourceEntity = typename SourceTable::Entity;
    using Field = binpack::detail::memberOf<RefFieldPtr>;
    using TargetField = binpack::detail::memberOf<TargetFieldPtr>;
    using ComparableField = ForeignKeyComparableType<Field>;
    using ComparableTargetField = std::conditional_t<isRef<Field>, RowId, ForeignKeyComparableType<TargetField>>;
    static_assert(std::is_same_v < binpack::detail::classOf < RefFieldPtr >,
    SourceEntity >, "restrict ref field must belong to the source entity"
    )
    ;
    static_assert(std::is_same_v < binpack::detail::classOf < TargetFieldPtr >,
    Entity >, "restrict target field must belong to the target entity"
    )
    ;
    static_assert(
        !isRef<Field> || sameMemberPointer<TargetFieldPtr, PrimaryKeyPtr>(),
        "Ref restrict delete currently requires target primary key"
    );
    static_assert(
        requires(const ComparableField& field, const ComparableTargetField& targetField) {
            { field == targetField } -> std::convertible_to<bool>;
        },
        "restrict fields must be comparable"
    );
    static_assert(!isRef<TargetField>, "restrict target field cannot be akkaradb::Ref<T>");

    const std::string_view fieldName = binpack::detail::memberName<RefFieldPtr>();
    for (const auto& restrictDelete : restrictDeletes_) {
        if (restrictDelete.source == &source && restrictDelete.fieldName == fieldName && restrictDelete.targetFieldName ==
            binpack::detail::memberName<TargetFieldPtr>()) { return *this; }
    }

    (void)source.template index<RefFieldPtr>();
    restrictDeletes_.push_back(
        RestrictDeleteDef{
            &source,
            this,
            std::string(fieldName),
            std::string(binpack::detail::memberName<TargetFieldPtr>()),
            [](const Entity& targetEntity, void* rawSource, void* rawTarget) {
                auto* sourceTable = static_cast<SourceTable*>(rawSource);
                auto* targetTable = static_cast<PackedTable*>(rawTarget);
                const auto targetValue = [&]() -> ComparableTargetField {
                    if constexpr (isRef<Field>) { return *targetTable->rowIdOf(targetEntity.*TargetFieldPtr); }
                    else { return foreignKeyComparable(targetEntity.*TargetFieldPtr); }
                }();
                bool hasReferences = false;
                auto scan = sourceTable->scanAll();
                while (scan.hasNext()) {
                    auto entry = scan.next();
                    const auto& fieldValue = entry.value.*RefFieldPtr;
                    if (!foreignKeyHasValue(fieldValue)) { continue; }
                    if (foreignKeyComparable(fieldValue) == targetValue) {
                        hasReferences = true;
                        break;
                    }
                }
                if (hasReferences) {
                    throw std::runtime_error("AkkaraDB foreign key: delete restricted by referencing entities");
                }
            }
        }
    );
    return *this;
}

template <auto RefFieldPtr, auto SourcePrimaryKeyPtr, auto TargetFieldPtr = PrimaryKeyPtr>
PackedTable& setNullDeleteFrom(PackedTable<SourcePrimaryKeyPtr>& source) {
    using SourceTable = PackedTable<SourcePrimaryKeyPtr>;
    using SourceEntity = typename SourceTable::Entity;
    using Field = binpack::detail::memberOf<RefFieldPtr>;
    using TargetField = binpack::detail::memberOf<TargetFieldPtr>;
    using ComparableField = ForeignKeyComparableType<Field>;
    using ComparableTargetField = std::conditional_t<isRef<Field>, RowId, ForeignKeyComparableType<TargetField>>;
    static_assert(std::is_same_v < binpack::detail::classOf < RefFieldPtr >,
    SourceEntity >, "set null ref field must belong to the source entity"
    )
    ;
    static_assert(std::is_same_v < binpack::detail::classOf < TargetFieldPtr >,
    Entity >, "set null target field must belong to the target entity"
    )
    ;
    static_assert(
        !isRef<Field> || sameMemberPointer<TargetFieldPtr, PrimaryKeyPtr>(),
        "Ref set null delete currently requires target primary key"
    );
    static_assert(
        requires(const ComparableField& field, const ComparableTargetField& targetField) {
            { field == targetField } -> std::convertible_to<bool>;
        },
        "set null fields must be comparable"
    );

    if constexpr (!query::isOptional<Field>) {
        throw std::invalid_argument("AkkaraDB foreign key: OnDelete::SetNull requires std::optional foreign key fields");
    }
    else {
        const std::string_view fieldName = binpack::detail::memberName<RefFieldPtr>();
        for (const auto& setNullDelete : setNullDeletes_) {
            if (setNullDelete.source == &source && setNullDelete.fieldName == fieldName && setNullDelete.targetFieldName ==
                binpack::detail::memberName<TargetFieldPtr>()) { return *this; }
        }

        (void)source.template index<RefFieldPtr>();
        setNullDeletes_.push_back(
            SetNullDeleteDef{
                &source,
                this,
                std::string(fieldName),
                std::string(binpack::detail::memberName<TargetFieldPtr>()),
                [](const Entity& targetEntity, void* rawSource, void* rawTarget) {
                    auto* sourceTable = static_cast<SourceTable*>(rawSource);
                    auto* targetTable = static_cast<PackedTable*>(rawTarget);
                    std::vector < typename SourceTable::PK > updateKeys;
                    const auto targetValue = [&]() -> ComparableTargetField {
                        if constexpr (isRef<Field>) { return *targetTable->rowIdOf(targetEntity.*TargetFieldPtr); }
                        else { return foreignKeyComparable(targetEntity.*TargetFieldPtr); }
                    }();
                    auto scan = sourceTable->scanAll();
                    while (scan.hasNext()) {
                        auto entry = scan.next();
                        const auto& fieldValue = entry.value.*RefFieldPtr;
                        if (!foreignKeyHasValue(fieldValue)) { continue; }
                        if (foreignKeyComparable(fieldValue) == targetValue) { updateKeys.push_back(entry.id); }
                    }
                    for (const auto& key : updateKeys) {
                        auto entity = sourceTable->get(key);
                        if (!entity) { continue; }
                        if (!foreignKeyHasValue((*entity).*RefFieldPtr)) { continue; }
                        setForeignKeyNull((*entity).*RefFieldPtr);
                        sourceTable->put(*entity);
                    }
                }
            }
        );
        return *this;
    }
}

template <auto RefFieldPtr, auto SourcePrimaryKeyPtr, auto TargetFieldPtr = PrimaryKeyPtr>
PackedTable& cascadeUpdateFrom(PackedTable<SourcePrimaryKeyPtr>& source) {
    using SourceTable = PackedTable<SourcePrimaryKeyPtr>;
    using SourceEntity = typename SourceTable::Entity;
    using Field = binpack::detail::memberOf<RefFieldPtr>;
    using TargetField = binpack::detail::memberOf<TargetFieldPtr>;
    using ComparableField = ForeignKeyComparableType<Field>;
    using ComparableTargetField = std::conditional_t<isRef<Field>, RowId, ForeignKeyComparableType<TargetField>>;
    static_assert(std::is_same_v < binpack::detail::classOf < RefFieldPtr >,
    SourceEntity >, "cascade update field must belong to the source entity"
    )
    ;
    static_assert(std::is_same_v < binpack::detail::classOf < TargetFieldPtr >,
    Entity >, "cascade update target field must belong to the target entity"
    )
    ;
    static_assert(
        requires(const ComparableField& field, const ComparableTargetField& targetField) {
            { field == targetField } -> std::convertible_to<bool>;
        },
        "cascade update fields must be comparable"
    );

    const std::string_view fieldName = binpack::detail::memberName<RefFieldPtr>();
    for (const auto& cascade : cascadeUpdates_) {
        if (cascade.source == &source && cascade.fieldName == fieldName && cascade.targetFieldName ==
            binpack::detail::memberName<TargetFieldPtr>()) { return *this; }
    }
    if constexpr (isRef<Field>) { return *this; }

    updateActionsNeedTargetWrite_ = true;
    cascadeUpdates_.push_back(
        UpdateCascadeDef{
            &source,
            this,
            std::string(fieldName),
            std::string(binpack::detail::memberName<TargetFieldPtr>()),
            [](const Entity& oldTargetEntity, const Entity& newTargetEntity, void* rawSource, void* rawTarget) {
                auto* sourceTable = static_cast<SourceTable*>(rawSource);
                (void)rawTarget;
                const auto oldTargetValue = [&]() -> ComparableTargetField {
                    return foreignKeyComparable(oldTargetEntity.*TargetFieldPtr);
                }();
                const auto newTargetValue = [&]() -> ComparableTargetField {
                    return foreignKeyComparable(newTargetEntity.*TargetFieldPtr);
                }();
                if (oldTargetValue == newTargetValue) { return; }

                std::vector < typename SourceTable::PK > updateKeys;
                auto scan = sourceTable->scanAll();
                while (scan.hasNext()) {
                    auto entry = scan.next();
                    const auto& fieldValue = entry.value.*RefFieldPtr;
                    if (!foreignKeyHasValue(fieldValue)) { continue; }
                    if (foreignKeyComparable(fieldValue) == oldTargetValue) { updateKeys.push_back(entry.id); }
                }
                for (const auto& key : updateKeys) {
                    auto entity = sourceTable->get(key);
                    if (!entity) { continue; }
                    (*entity).*RefFieldPtr = newTargetEntity.*TargetFieldPtr;
                    sourceTable->put(*entity);
                }
            }
        }
    );
    return *this;
}

template <auto RefFieldPtr, auto SourcePrimaryKeyPtr, auto TargetFieldPtr = PrimaryKeyPtr>
PackedTable& restrictUpdateFrom(PackedTable<SourcePrimaryKeyPtr>& source) {
    using SourceTable = PackedTable<SourcePrimaryKeyPtr>;
    using SourceEntity = typename SourceTable::Entity;
    using Field = binpack::detail::memberOf<RefFieldPtr>;
    using TargetField = binpack::detail::memberOf<TargetFieldPtr>;
    using ComparableField = ForeignKeyComparableType<Field>;
    using ComparableTargetField = std::conditional_t<isRef<Field>, RowId, ForeignKeyComparableType<TargetField>>;
    static_assert(std::is_same_v < binpack::detail::classOf < RefFieldPtr >,
    SourceEntity >, "restrict update field must belong to the source entity"
    )
    ;
    static_assert(std::is_same_v < binpack::detail::classOf < TargetFieldPtr >,
    Entity >, "restrict update target field must belong to the target entity"
    )
    ;
    static_assert(
        requires(const ComparableField& field, const ComparableTargetField& targetField) {
            { field == targetField } -> std::convertible_to<bool>;
        },
        "restrict update fields must be comparable"
    );

    const std::string_view fieldName = binpack::detail::memberName<RefFieldPtr>();
    for (const auto& restrictUpdate : restrictUpdates_) {
        if (restrictUpdate.source == &source && restrictUpdate.fieldName == fieldName && restrictUpdate.targetFieldName ==
            binpack::detail::memberName<TargetFieldPtr>()) { return *this; }
    }
    if constexpr (isRef<Field>) { return *this; }

    restrictUpdates_.push_back(
        UpdateRestrictDef{
            &source,
            this,
            std::string(fieldName),
            std::string(binpack::detail::memberName<TargetFieldPtr>()),
            [](const Entity& oldTargetEntity, const Entity& newTargetEntity, void* rawSource, void* rawTarget) {
                auto* sourceTable = static_cast<SourceTable*>(rawSource);
                (void)rawTarget;

                const auto oldTargetValue = foreignKeyComparable(oldTargetEntity.*TargetFieldPtr);
                const auto newTargetValue = foreignKeyComparable(newTargetEntity.*TargetFieldPtr);
                if (oldTargetValue == newTargetValue) { return; }

                auto scan = sourceTable->scanAll();
                while (scan.hasNext()) {
                    auto entry = scan.next();
                    const auto& fieldValue = entry.value.*RefFieldPtr;
                    if (!foreignKeyHasValue(fieldValue)) { continue; }
                    if (foreignKeyComparable(fieldValue) == oldTargetValue) {
                        throw std::runtime_error("AkkaraDB foreign key: update restricted by referencing entities");
                    }
                }
            }
        }
    );
    return *this;
}

template <auto RefFieldPtr, auto SourcePrimaryKeyPtr, auto TargetFieldPtr = PrimaryKeyPtr>
PackedTable& setNullUpdateFrom(PackedTable<SourcePrimaryKeyPtr>& source) {
    using SourceTable = PackedTable<SourcePrimaryKeyPtr>;
    using SourceEntity = typename SourceTable::Entity;
    using Field = binpack::detail::memberOf<RefFieldPtr>;
    using TargetField = binpack::detail::memberOf<TargetFieldPtr>;
    using ComparableField = ForeignKeyComparableType<Field>;
    using ComparableTargetField = std::conditional_t<isRef<Field>, RowId, ForeignKeyComparableType<TargetField>>;
    static_assert(std::is_same_v < binpack::detail::classOf < RefFieldPtr >,
    SourceEntity >, "set null update field must belong to the source entity"
    )
    ;
    static_assert(std::is_same_v < binpack::detail::classOf < TargetFieldPtr >,
    Entity >, "set null update target field must belong to the target entity"
    )
    ;
    static_assert(
        requires(const ComparableField& field, const ComparableTargetField& targetField) {
            { field == targetField } -> std::convertible_to<bool>;
        },
        "set null update fields must be comparable"
    );

    if constexpr (!query::isOptional<Field>) {
        throw std::invalid_argument("AkkaraDB foreign key: OnUpdate::SetNull requires std::optional foreign key fields");
    }
    else {
        const std::string_view fieldName = binpack::detail::memberName<RefFieldPtr>();
        for (const auto& setNullUpdate : setNullUpdates_) {
            if (setNullUpdate.source == &source && setNullUpdate.fieldName == fieldName && setNullUpdate.targetFieldName ==
                binpack::detail::memberName<TargetFieldPtr>()) { return *this; }
        }
        if constexpr (isRef<Field>) { return *this; }

        updateActionsNeedTargetWrite_ = true;
        setNullUpdates_.push_back(
            UpdateSetNullDef{
                &source,
                this,
                std::string(fieldName),
                std::string(binpack::detail::memberName<TargetFieldPtr>()),
                [](const Entity& oldTargetEntity, const Entity& newTargetEntity, void* rawSource, void* rawTarget) {
                    auto* sourceTable = static_cast<SourceTable*>(rawSource);
                    (void)rawTarget;
                    const auto oldTargetValue = foreignKeyComparable(oldTargetEntity.*TargetFieldPtr);
                    const auto newTargetValue = foreignKeyComparable(newTargetEntity.*TargetFieldPtr);
                    if (oldTargetValue == newTargetValue) { return; }

                    std::vector < typename SourceTable::PK > updateKeys;
                    auto scan = sourceTable->scanAll();
                    while (scan.hasNext()) {
                        auto entry = scan.next();
                        const auto& fieldValue = entry.value.*RefFieldPtr;
                        if (!foreignKeyHasValue(fieldValue)) { continue; }
                        if (foreignKeyComparable(fieldValue) == oldTargetValue) { updateKeys.push_back(entry.id); }
                    }
                    for (const auto& key : updateKeys) {
                        auto entity = sourceTable->get(key);
                        if (!entity) { continue; }
                        if (!foreignKeyHasValue((*entity).*RefFieldPtr)) { continue; }
                        setForeignKeyNull((*entity).*RefFieldPtr);
                        sourceTable->put(*entity);
                    }
                }
            }
        );
        return *this;
    }
}

private:
void runCascadeDeletes(const Entity& entity) {
    for (const auto& cascade : cascadeDeletes_) { cascade.cascade(entity, cascade.source, cascade.target); }
}

void runRestrictDeletes(const Entity& entity) {
    for (const auto& restrictDelete : restrictDeletes_) {
        restrictDelete.restrictDelete(entity, restrictDelete.source, restrictDelete.target);
    }
}

void runSetNullDeletes(const Entity& entity) {
    for (const auto& setNullDelete : setNullDeletes_) {
        setNullDelete.setNullDelete(entity, setNullDelete.source, setNullDelete.target);
    }
}

void runCascadeUpdates(const Entity& oldEntity, const Entity& newEntity) {
    for (const auto& cascadeUpdate : cascadeUpdates_) {
        cascadeUpdate.cascadeUpdate(oldEntity, newEntity, cascadeUpdate.source, cascadeUpdate.target);
    }
}

void runRestrictUpdates(const Entity& oldEntity, const Entity& newEntity) {
    for (const auto& restrictUpdate : restrictUpdates_) {
        restrictUpdate.restrictUpdate(oldEntity, newEntity, restrictUpdate.source, restrictUpdate.target);
    }
}

void runSetNullUpdates(const Entity& oldEntity, const Entity& newEntity) {
    for (const auto& setNullUpdate : setNullUpdates_) {
        setNullUpdate.setNullUpdate(oldEntity, newEntity, setNullUpdate.source, setNullUpdate.target);
    }
}

public:

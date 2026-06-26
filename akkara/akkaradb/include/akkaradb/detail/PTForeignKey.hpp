/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkaradb/include/akkaradb/detail/PTForeignKey.hpp
#pragma once

template <auto FieldPtr, auto TargetPrimaryKeyPtr, auto TargetFieldPtr = TargetPrimaryKeyPtr>
PackedTable& foreignKey(PackedTable<TargetPrimaryKeyPtr>& target) {
    static_assert(std::is_same_v < binpack::detail::classOf < FieldPtr >,
    Entity >, "foreign key field must belong to the table entity"
    )
    ;
    using Field = binpack::detail::memberOf<FieldPtr>;
    using TargetTable = PackedTable<TargetPrimaryKeyPtr>;
    using TargetEntity = typename TargetTable::Entity;
    using TargetField = binpack::detail::memberOf<TargetFieldPtr>;
    using ComparableField = ForeignKeyComparableType<Field>;
    using ComparableTargetField = ForeignKeyComparableType<TargetField>;
    static_assert(std::is_same_v < binpack::detail::classOf < TargetFieldPtr >,
    TargetEntity >, "foreign key target field must belong to the target table entity"
    )
    ;
    static_assert(
        requires(const ComparableField& field, const ComparableTargetField& targetField) {
            { field == targetField } -> std::convertible_to<bool>;
        },
        "foreign key fields must be comparable"
    );
    static_assert(!isRef<TargetField>, "foreign key target field cannot be akkaradb::Ref<T>");

    const std::string_view fieldName = binpack::detail::memberName<FieldPtr>();
    for (const auto& fk : foreignKeys_) { if (fk.fieldName == fieldName) { return *this; } }

    if constexpr (!sameMemberPointer<TargetFieldPtr, TargetPrimaryKeyPtr>()) { (void)target.template index<TargetFieldPtr>(); }

    foreignKeys_.push_back(
        ForeignKeyDef{
            std::string(fieldName),
            &target,
            [](const Entity& entity, void* rawTarget) {
                auto* targetTable = static_cast<TargetTable*>(rawTarget);
                const auto& field = entity.*FieldPtr;
                if (!foreignKeyHasValue(field)) { return; }
                const auto& value = foreignKeyComparable(entity.*FieldPtr);
                bool exists = false;
                if constexpr (sameMemberPointer<TargetFieldPtr, TargetPrimaryKeyPtr>()) {
                    if constexpr (isRef<Field>) { exists = targetTable->primaryKeyOf(value).has_value(); }
                    else { exists = targetTable->exists(value); }
                }
                else { exists = targetTable->template hasAnyByIndexedFieldValue<TargetFieldPtr>(value); }
                if (!exists) { throw std::runtime_error("AkkaraDB foreign key: referenced entity was not found"); }
            }
        }
    );
    return *this;
}

template <auto FieldPtr>
PackedTable& foreignKey() {
    static_assert(std::is_same_v < binpack::detail::classOf < FieldPtr >,
    Entity >, "foreign key field must belong to the table entity"
    )
    ;
    using Field = binpack::detail::memberOf<FieldPtr>;
    static_assert(isRef<Field>, "foreignKey field must be akkaradb::Ref<T>");

    const std::string_view fieldName = binpack::detail::memberName<FieldPtr>();
    for (const auto& fk : foreignKeys_) { if (fk.fieldName == fieldName) { return *this; } }

    foreignKeys_.push_back(
        ForeignKeyDef{
            std::string(fieldName),
            nullptr,
            [](const Entity& entity, void* rawTable) {
                auto* table = static_cast<const PackedTable*>(rawTable);
                using Target = typename RefTarget<Field>::Type;
                const auto* binding = table->template findRefBinding<Target>();
                if (binding == nullptr) {
                    throw std::runtime_error("AkkaraDB foreign key: Ref target table is not registered");
                }

                const auto& ref = entity.*FieldPtr;
                const bool exists = ref.hasRowId() ? binding->existsByRowId(ref.rowId()) : binding->exists(ref.id());
                if (!exists) {
                    throw std::runtime_error(
                        ref.hasRowId()
                            ? "AkkaraDB foreign key: referenced entity was not found (row id)"
                            : "AkkaraDB foreign key: referenced entity was not found (key)"
                    );
                }
            }
        }
    );
    return *this;
}

private:
template <typename X>
static decltype(auto) foreignKeyComparable(const X& value) {
    using Field = std::remove_cvref_t<X>;
    if constexpr (query::isOptional<Field>) { return foreignKeyComparable(*value); }
    else if constexpr (isImmutableField<Field>) { return foreignKeyComparable(value.get()); }
    else if constexpr (isRef<Field>) { return value.rowId(); }
    else { return (value); }
}

template <typename X>
[[nodiscard]] static bool foreignKeyHasValue(const X& value) {
    using Field = std::remove_cvref_t<X>;
    if constexpr (query::isOptional<Field>) { return value.has_value(); }
    else { return true; }
}

template <typename X>
static void setForeignKeyNull(X& value) {
    using Field = std::remove_cvref_t<X>;
    static_assert(query::isOptional<Field>, "setForeignKeyNull requires std::optional foreign key fields");
    value = std::nullopt;
}

void validateForeignKeys(const Entity& entity) const {
    for (const auto& fk : foreignKeys_) {
        fk.validate(entity, fk.target == nullptr ? const_cast<PackedTable*>(this) : fk.target);
    }
}

public:

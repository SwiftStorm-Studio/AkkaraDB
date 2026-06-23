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

// akkaradb/include/akkaradb/detail/PTRefs.hpp
#pragma once

template <auto FieldPtr, auto TargetPrimaryKeyPtr>
PackedTable& bindRef(PackedTable<TargetPrimaryKeyPtr>& target) {
    static_assert(std::is_same_v < binpack::detail::classOf < FieldPtr >,
    Entity >, "ref field must belong to the table entity"
    )
    ;
    using Field = binpack::detail::memberOf<FieldPtr>;
    static_assert(isRef<Field>, "bindRef field must be akkaradb::Ref<T>");
    using Target = typename RefTarget<Field>::Type;
    using TargetTable = PackedTable<TargetPrimaryKeyPtr>;
    static_assert(std::is_same_v < typename TargetTable::Entity,
    Target >, "ref target table entity does not match Ref<T>"
    )
    ;
    static_assert(
        std::is_same_v<typename Field::Key, typename TargetTable::PK>,
        "ref key type does not match target table primary key"
    );

    const std::string_view fieldName = binpack::detail::memberName<FieldPtr>();
    for (auto& refField : refFields_) {
        if (refField.fieldName == fieldName) {
            auto binding = std::make_unique<TableRefBinding<TargetPrimaryKeyPtr>>(&target);
            refField.binding = binding.get();
            refBindings_.push_back(std::move(binding));
            return *this;
        }
    }

    auto binding = std::make_unique<TableRefBinding<TargetPrimaryKeyPtr>>(&target);
    auto* rawBinding = binding.get();
    refBindings_.push_back(std::move(binding));
    refFields_.push_back(
        RefFieldDef{
            std::string(fieldName),
            rawBinding,
            [](const Entity& entity, void* raw) {
                auto* binding = static_cast<TableRefBinding<TargetPrimaryKeyPtr>*>(raw);
                (entity.*FieldPtr).attach(binding);
            },
            [](const Entity& entity, void* raw) {
                auto* binding = static_cast<TableRefBinding<TargetPrimaryKeyPtr>*>(raw);
                const auto& ref = entity.*FieldPtr;
                ref.attach(binding);
                if (ref.dirty()) {
                    const auto key = RefTraits<Target>::keyOf(ref.value());
                    binding->put(ref.value());
                    if (const auto rowId = binding->rowIdOf(key)) { ref.rememberRowId(*rowId); }
                    ref.markClean();
                }
            }
        }
    );
    return *this;
}

PackedTable& bindRefsFrom(const RefBindingLookup& lookup) {
    refBindingLookup_ = &lookup;
    return *this;
}

private:
void attachRefBindings(const Entity& entity) const {
    if (refFields_.empty() && refBindingLookup_ == nullptr) { return; }
    for (const auto& refField : refFields_) { refField.attach(entity, refField.binding); }
    if (refBindingLookup_ != nullptr) { attachAutoRefs(entity); }
}

void flushDirtyRefs(const Entity& entity) const {
    if (refFields_.empty() && refBindingLookup_ == nullptr) { return; }
    for (const auto& refField : refFields_) { refField.flush(entity, refField.binding); }
    if (refBindingLookup_ != nullptr) { flushAutoRefs(entity); }
}

template <typename Target>
[[nodiscard]] const RefBinding<Target>* findRefBinding() const {
    if (refBindingLookup_ == nullptr) { return nullptr; }
    auto* raw = refBindingLookup_->findRefBinding(std::type_index(typeid(Target)));
    if (raw == nullptr) { return nullptr; }
    return static_cast<RefBinding<Target>*>(raw);
}

template <typename X>
void attachAutoRefs(const X& value) const {
    using Field = std::remove_cvref_t<X>;
    if constexpr (isRef<Field>) {
        using Target = typename RefTarget<Field>::Type;
        auto* raw = refBindingLookup_->findRefBinding(std::type_index(typeid(Target)));
        if (raw == nullptr) { throw std::runtime_error("AkkaraDB schema: Ref target table is not registered"); }
        value.attach(static_cast<RefBinding<Target>*>(raw));
    }
    else if constexpr (std::is_aggregate_v<Field> && !std::is_array_v<Field>) {
        boost::pfr::for_each_field(value, [this](const auto& field) { attachAutoRefs(field); });
    }
}

template <typename X>
void flushAutoRefs(const X& value) const {
    using Field = std::remove_cvref_t<X>;
    if constexpr (isRef<Field>) {
        using Target = typename RefTarget<Field>::Type;
        auto* raw = refBindingLookup_->findRefBinding(std::type_index(typeid(Target)));
        if (raw == nullptr) { throw std::runtime_error("AkkaraDB schema: Ref target table is not registered"); }
        auto* binding = static_cast<RefBinding<Target>*>(raw);
        value.attach(binding);
        if (value.dirty()) {
            const auto key = RefTraits<Target>::keyOf(value.value());
            binding->put(value.value());
            const auto rowId = binding->rowIdOf(key);
            if (!rowId.has_value()) { throw std::runtime_error("AkkaraDB Ref: dirty ref flush missing row id after put"); }
            value.rememberRowId(*rowId);
            value.markClean();
        }
    }
    else if constexpr (std::is_aggregate_v<Field> && !std::is_array_v<Field>) {
        boost::pfr::for_each_field(value, [this](const auto& field) { flushAutoRefs(field); });
    }
}

public:

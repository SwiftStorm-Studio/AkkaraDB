/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 */

// akkaradb/include/akkaradb/Ref.hpp
#pragma once

#include <concepts>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <typeindex>
#include <utility>

namespace akkaradb {
    using RowId = uint64_t;

    template <typename T>
    class Immutable {
        public:
            using ValueType = T;

            Immutable() = default;

            template <typename U> requires std::constructible_from<T, U&&>
            Immutable(U&& value) : value_{std::forward<U>(value)} {}

            [[nodiscard]] static Immutable persisted(const T& value) {
                Immutable out{value};
                out.sealed_ = true;
                return out;
            }

            [[nodiscard]] static Immutable persisted(T&& value) {
                Immutable out{std::move(value)};
                out.sealed_ = true;
                return out;
            }

            template <typename U> requires std::constructible_from<T, U&&>
            Immutable& operator=(U&& value) {
                T next{std::forward<U>(value)};
                if (sealed_ && !(value_ == next)) { throw std::runtime_error("AkkaraDB Immutable: persisted field cannot be modified"); }
                value_ = std::move(next);
                return *this;
            }

            [[nodiscard]] const T& get() const noexcept { return value_; }
            [[nodiscard]] operator const T&() const noexcept { return value_; }

            [[nodiscard]] const T& value() const noexcept { return value_; }

            [[nodiscard]] T& value() {
                if (sealed_) { throw std::runtime_error("AkkaraDB Immutable: persisted field cannot be modified"); }
                return value_;
            }

            [[nodiscard]] const T* operator->() const noexcept { return &value_; }

            [[nodiscard]] T* operator->() {
                if (sealed_) { throw std::runtime_error("AkkaraDB Immutable: persisted field cannot be modified"); }
                return &value_;
            }

            [[nodiscard]] const T& operator*() const noexcept { return value_; }

            [[nodiscard]] T& operator*() {
                if (sealed_) { throw std::runtime_error("AkkaraDB Immutable: persisted field cannot be modified"); }
                return value_;
            }

            void seal() const noexcept { sealed_ = true; }
            [[nodiscard]] bool sealed() const noexcept { return sealed_; }

            void replacePersisted(T value) noexcept(std::is_nothrow_move_assignable_v<T>) {
                value_ = std::move(value);
                sealed_ = true;
            }

            [[nodiscard]] friend bool operator==(const Immutable&, const Immutable&) = default;

        private:
            T value_{};
            mutable bool sealed_ = false;
    };

    template <typename T>
    using Const = Immutable<T>;

    template <typename T>
    struct RefTraits;

    template <typename T, typename = void>
    struct HasRefTraits : std::false_type {};

    template <typename T>
    struct HasRefTraits<T, std::void_t < typename RefTraits<T>::Key>
    >
    :
    std::true_type {};

    template <typename T>
    inline constexpr bool hasRefTraits = HasRefTraits<T>::value;

    class RefBindingBase {
        public:
            virtual ~RefBindingBase() = default;
    };

    template <typename T>
    class RefBinding : public RefBindingBase {
        public:
            using Key = typename RefTraits<T>::Key;

            [[nodiscard]] virtual bool exists(const Key& key) const = 0;
            [[nodiscard]] virtual std::optional<T> get(const Key& key) const = 0;
            [[nodiscard]] virtual bool existsByRowId(RowId rowId) const = 0;
            [[nodiscard]] virtual std::optional<RowId> rowIdOf(const Key& key) const = 0;
            [[nodiscard]] virtual std::optional<Key> keyOfRowId(RowId rowId) const = 0;
            [[nodiscard]] virtual std::optional<T> getByRowId(RowId rowId) const = 0;
            virtual void put(const T& value) = 0;
    };

    class RefBindingLookup {
        public:
            virtual ~RefBindingLookup() = default;
            [[nodiscard]] virtual RefBindingBase* findRefBinding(std::type_index entityType) const = 0;
    };

    template <typename T>
    class Ref {
        public:
            using Entity = T;
            using Key = typename RefTraits<T>::Key;

            Ref() = default;
            explicit Ref(Key id) : key_{std::move(id)}, keyKnown_{true} {}

            [[nodiscard]] static Ref fromRowId(RowId rowId) {
                Ref out;
                out.rowId_ = rowId;
                out.rowIdKnown_ = rowId != 0;
                return out;
            }

            Ref(const T& value) : key_{RefTraits<T>::keyOf(value)}, value_{value}, loaded_{true}, dirty_{true}, keyKnown_{true} {}

            Ref(T&& value) : key_{RefTraits<T>::keyOf(value)}, value_{std::move(value)}, loaded_{true}, dirty_{true}, keyKnown_{true} {}

            Ref& operator=(const Key& id) {
                key_ = id;
                value_.reset();
                loaded_ = false;
                dirty_ = false;
                rowId_ = 0;
                rowIdKnown_ = false;
                keyKnown_ = true;
                return *this;
            }

            Ref& operator=(const T& value) {
                key_ = RefTraits<T>::keyOf(value);
                value_ = value;
                loaded_ = true;
                dirty_ = true;
                rowId_ = 0;
                rowIdKnown_ = false;
                keyKnown_ = true;
                return *this;
            }

            Ref& operator=(T&& value) {
                key_ = RefTraits<T>::keyOf(value);
                value_ = std::move(value);
                loaded_ = true;
                dirty_ = true;
                rowId_ = 0;
                rowIdKnown_ = false;
                keyKnown_ = true;
                return *this;
            }

            [[nodiscard]] const Key& id() const {
                ensureKeyKnown();
                return key_;
            }

            [[nodiscard]] bool loaded() const noexcept { return loaded_; }
            [[nodiscard]] bool dirty() const noexcept { return dirty_; }
            [[nodiscard]] bool attached() const noexcept { return binding_ != nullptr; }
            [[nodiscard]] bool hasRowId() const noexcept { return rowIdKnown_; }

            [[nodiscard]] RowId rowId() const {
                ensureRowIdKnown();
                return rowId_;
            }

            void attach(RefBinding<T>* binding) const noexcept { binding_ = binding; }

            void rememberRowId(RowId rowId) const noexcept {
                rowId_ = rowId;
                rowIdKnown_ = rowId != 0;
            }

            void markClean() const noexcept { dirty_ = false; }

            [[nodiscard]] const T& value() const {
                ensureLoaded();
                return *value_;
            }

            [[nodiscard]] T& value() {
                ensureLoaded();
                dirty_ = true;
                return *value_;
            }

            [[nodiscard]] const T* operator->() const {
                ensureLoaded();
                return &*value_;
            }

            [[nodiscard]] T* operator->() {
                ensureLoaded();
                dirty_ = true;
                return &*value_;
            }

            [[nodiscard]] const T& operator*() const { return value(); }
            [[nodiscard]] T& operator*() { return value(); }

        private:
            void ensureKeyKnown() const {
                if (keyKnown_) { return; }
                if (loaded_) {
                    key_ = RefTraits<T>::keyOf(*value_);
                    keyKnown_ = true;
                    return;
                }
                if (binding_ == nullptr || !rowIdKnown_) { throw std::runtime_error("AkkaraDB Ref: key is not available"); }
                auto resolvedKey = binding_->keyOfRowId(rowId_);
                if (!resolvedKey) { throw std::runtime_error("AkkaraDB Ref: referenced entity was not found"); }
                key_ = std::move(*resolvedKey);
                keyKnown_ = true;
            }

            void ensureRowIdKnown() const {
                if (rowIdKnown_) { return; }
                if (binding_ == nullptr) { throw std::runtime_error("AkkaraDB Ref: detached reference cannot resolve row id"); }
                ensureKeyKnown();
                auto resolvedRowId = binding_->rowIdOf(key_);
                if (!resolvedRowId) { throw std::runtime_error("AkkaraDB Ref: referenced entity was not found"); }
                rowId_ = *resolvedRowId;
                rowIdKnown_ = true;
            }

            void ensureLoaded() const {
                if (loaded_) { return; }
                if (binding_ == nullptr) { throw std::runtime_error("AkkaraDB Ref: detached reference cannot be resolved"); }
                std::optional<T> resolved;
                if (rowIdKnown_) { resolved = binding_->getByRowId(rowId_); }
                else {
                    ensureKeyKnown();
                    resolved = binding_->get(key_);
                }
                if (!resolved) { throw std::runtime_error("AkkaraDB Ref: referenced entity was not found"); }
                value_ = std::move(*resolved);
                key_ = RefTraits<T>::keyOf(*value_);
                keyKnown_ = true;
                loaded_ = true;
            }

            mutable Key key_{};
            mutable RowId rowId_ = 0;
            mutable std::optional<T> value_;
            mutable RefBinding<T>* binding_ = nullptr;
            mutable bool loaded_ = false;
            mutable bool dirty_ = false;
            mutable bool rowIdKnown_ = false;
            mutable bool keyKnown_ = false;
    };

    template <typename T>
    [[nodiscard]] Ref<T> ref(typename Ref<T>::Key id) { return Ref<T>{std::move(id)}; }

    template <typename T>
    [[nodiscard]] Ref<T> ref(const T& value) { return Ref<T>{value}; }

    template <typename T>
    struct IsRef : std::false_type {};

    template <typename T>
    struct IsRef<Ref<T>> : std::true_type {};

    template <typename T>
    inline constexpr bool isRef = IsRef<std::remove_cvref_t<T>>::value;

    template <typename T>
    struct IsImmutableField : std::false_type {};

    template <typename T>
    struct IsImmutableField<Immutable<T>> : std::true_type {};

    template <typename T>
    inline constexpr bool isImmutableField = IsImmutableField<std::remove_cvref_t<T>>::value;

    template <typename T>
    struct ImmutableValue;

    template <typename T>
    struct ImmutableValue<Immutable<T>> {
        using Type = T;
    };

    template <typename T>
    struct RefTarget;

    template <typename T>
    struct RefTarget<Ref<T>> {
        using Type = T;
    };
} // namespace akkaradb

#define AKKARADB_REF_ENTITY(Type, Field) \
    template <> struct ::akkaradb::RefTraits<Type> { \
        using Key = std::remove_cv_t<decltype(Type::Field)>; \
        static constexpr auto primaryKey = &Type::Field; \
        [[nodiscard]] static const Key& keyOf(const Type& value) noexcept { return value.Field; } \
    }

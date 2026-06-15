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

#include <optional>
#include <stdexcept>
#include <type_traits>
#include <typeindex>
#include <utility>

namespace akkaradb {
    template <typename T>
    struct RefTraits;

    template <typename T, typename = void>
    struct HasRefTraits : std::false_type {};

    template <typename T>
    struct HasRefTraits<T, std::void_t<typename RefTraits<T>::Key>> : std::true_type {};

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
            explicit Ref(Key id) : id_{std::move(id)} {}

            Ref(const T& value)
                : id_{RefTraits<T>::keyOf(value)},
                  value_{value},
                  loaded_{true},
                  dirty_{true} {}

            Ref(T&& value)
                : id_{RefTraits<T>::keyOf(value)},
                  value_{std::move(value)},
                  loaded_{true},
                  dirty_{true} {}

            Ref& operator=(const Key& id) {
                id_ = id;
                value_.reset();
                loaded_ = false;
                dirty_ = false;
                return *this;
            }

            Ref& operator=(const T& value) {
                id_ = RefTraits<T>::keyOf(value);
                value_ = value;
                loaded_ = true;
                dirty_ = true;
                return *this;
            }

            Ref& operator=(T&& value) {
                id_ = RefTraits<T>::keyOf(value);
                value_ = std::move(value);
                loaded_ = true;
                dirty_ = true;
                return *this;
            }

            [[nodiscard]] const Key& id() const noexcept { return id_; }
            [[nodiscard]] bool loaded() const noexcept { return loaded_; }
            [[nodiscard]] bool dirty() const noexcept { return dirty_; }
            [[nodiscard]] bool attached() const noexcept { return binding_ != nullptr; }

            void attach(RefBinding<T>* binding) const noexcept { binding_ = binding; }
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
            void ensureLoaded() const {
                if (loaded_) { return; }
                if (binding_ == nullptr) { throw std::runtime_error("AkkaraDB Ref: detached reference cannot be resolved"); }
                auto resolved = binding_->get(id_);
                if (!resolved) { throw std::runtime_error("AkkaraDB Ref: referenced entity was not found"); }
                value_ = std::move(*resolved);
                loaded_ = true;
            }

            Key id_{};
            mutable std::optional<T> value_;
            mutable RefBinding<T>* binding_ = nullptr;
            mutable bool loaded_ = false;
            mutable bool dirty_ = false;
    };

    template <typename T>
    [[nodiscard]] Ref<T> ref(typename Ref<T>::Key id) {
        return Ref<T>{std::move(id)};
    }

    template <typename T>
    [[nodiscard]] Ref<T> ref(const T& value) {
        return Ref<T>{value};
    }

    template <typename T>
    struct IsRef : std::false_type {};

    template <typename T>
    struct IsRef<Ref<T>> : std::true_type {};

    template <typename T>
    inline constexpr bool isRef = IsRef<std::remove_cvref_t<T>>::value;

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

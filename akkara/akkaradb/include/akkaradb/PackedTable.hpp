/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkaradb/include/akkaradb/PackedTable.hpp
#pragma once

#include "AkkQuery.hpp"
#include "binpack/BinPack.hpp"
#include "binpack/detail/MemberPtrTraits.hpp"
#include "detail/Hash.hpp"
#include "akk/engine/AkkEngine.hpp"
#include "akk/core/buffer/BufferArena.hpp"
#include "akk/core/record/KeyFingerprint.hpp"

#include <array>
#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <typeindex>
#include <unordered_set>
#include <utility>
#include <vector>

namespace akkaradb {
    #define AKKARADB_QUERYABLE_FIELD(Type, Field) ::akkaradb::query::Column<&Type::Field> Field{};
    #define AKKARADB_DETAIL_EMPTY()
    #define AKKARADB_DETAIL_DEFER(Id) Id AKKARADB_DETAIL_EMPTY()
    #define AKKARADB_DETAIL_OBSTRUCT(...) __VA_ARGS__ AKKARADB_DETAIL_DEFER(AKKARADB_DETAIL_EMPTY)()
    #define AKKARADB_DETAIL_EVAL(...) AKKARADB_DETAIL_EVAL1(AKKARADB_DETAIL_EVAL1(AKKARADB_DETAIL_EVAL1(__VA_ARGS__)))
    #define AKKARADB_DETAIL_EVAL1(...) AKKARADB_DETAIL_EVAL2(AKKARADB_DETAIL_EVAL2(AKKARADB_DETAIL_EVAL2(__VA_ARGS__)))
    #define AKKARADB_DETAIL_EVAL2(...) AKKARADB_DETAIL_EVAL3(AKKARADB_DETAIL_EVAL3(AKKARADB_DETAIL_EVAL3(__VA_ARGS__)))
    #define AKKARADB_DETAIL_EVAL3(...) AKKARADB_DETAIL_EVAL4(AKKARADB_DETAIL_EVAL4(AKKARADB_DETAIL_EVAL4(__VA_ARGS__)))
    #define AKKARADB_DETAIL_EVAL4(...) AKKARADB_DETAIL_EVAL5(AKKARADB_DETAIL_EVAL5(AKKARADB_DETAIL_EVAL5(__VA_ARGS__)))
    #define AKKARADB_DETAIL_EVAL5(...) __VA_ARGS__
    #define AKKARADB_DETAIL_FOR_EACH(Macro, Type, Field, ...) \
        Macro(Type, Field) \
        __VA_OPT__(AKKARADB_DETAIL_OBSTRUCT(AKKARADB_DETAIL_FOR_EACH_AGAIN)()(Macro, Type, __VA_ARGS__))
    #define AKKARADB_DETAIL_FOR_EACH_AGAIN() AKKARADB_DETAIL_FOR_EACH

    #define AKKARADB_QUERYABLE(Type, ...) \
        [[nodiscard]] inline auto akkaradbQueryProxy(::akkaradb::query::ProxyTag<Type>) { \
            struct Proxy { \
                AKKARADB_DETAIL_EVAL(AKKARADB_DETAIL_FOR_EACH(AKKARADB_QUERYABLE_FIELD, Type, __VA_ARGS__)) \
            }; \
            return Proxy{}; \
        }

    #define AKKARADB_ENTITY(Type, PrimaryKey, ...) \
        AKKARADB_REF_ENTITY(Type, PrimaryKey); \
        AKKARADB_QUERYABLE(Type, PrimaryKey __VA_OPT__(,) __VA_ARGS__)

    template <typename T>
    struct ForeignKeyValueTraits {
        using Type = std::remove_cvref_t<T>;
        static constexpr bool nullable = false;
    };

    template <typename T>
    struct ForeignKeyValueTraits<Immutable<T>> {
        using Type = typename ForeignKeyValueTraits<T>::Type;
        static constexpr bool nullable = false;
    };

    template <typename T>
    struct ForeignKeyValueTraits<Ref<T>> {
        using Type = RowId;
        static constexpr bool nullable = false;
    };

    template <typename T>
    struct ForeignKeyValueTraits<std::optional<T>> {
        using Type = typename ForeignKeyValueTraits<T>::Type;
        static constexpr bool nullable = true;
    };

    template <typename T>
    using ForeignKeyComparableType = typename ForeignKeyValueTraits<std::remove_cvref_t<T>>::Type;

    template <auto PrimaryKeyPtr>
    class PackedTable {
        class ArenaByteBuffer {
            public:
                ArenaByteBuffer() = default;
                explicit ArenaByteBuffer(core::BufferArena* arena) noexcept : arena_{arena} {}
                ArenaByteBuffer(const ArenaByteBuffer&) = default;
                ArenaByteBuffer(ArenaByteBuffer&&) noexcept = default;
                ArenaByteBuffer& operator=(ArenaByteBuffer&&) noexcept = default;

                ArenaByteBuffer& operator=(const ArenaByteBuffer& other) {
                    if (this != &other) {
                        clear();
                        insert(end(), other.begin(), other.end());
                    }
                    return *this;
                }

                void bind(BufferArena* arena) noexcept {
                    arena_ = arena;
                    release();
                }

                void release() noexcept {
                    data_ = nullptr;
                    size_ = 0;
                    capacity_ = 0;
                }

                void clear() noexcept { size_ = 0; }

                void reserve(size_t capacity) {
                    if (capacity <= capacity_) { return; }
                    ensureArena();
                    std::byte* raw = arena_->allocate(capacity, alignof(uint8_t));
                    auto* next = reinterpret_cast<uint8_t*>(raw);
                    if (data_ != nullptr && size_ != 0) { std::memcpy(next, data_, size_); }
                    data_ = next;
                    capacity_ = capacity;
                }

                void resize(size_t size) {
                    const size_t oldSize = size_;
                    reserve(size);
                    if (size > oldSize) { std::memset(data_ + oldSize, 0, size - oldSize); }
                    size_ = size;
                }

                template <typename It>
                void assign(It first, It last) {
                    clear();
                    insert(end(), first, last);
                }

                template <typename It>
                void insert(uint8_t*, It first, It last) {
                    const size_t add = static_cast<size_t>(std::distance(first, last));
                    reserve(size_ + add);
                    for (; first != last; ++first) { data_[size_++] = static_cast<uint8_t>(*first); }
                }

                void push_back(uint8_t value) {
                    reserve(size_ + 1);
                    data_[size_++] = value;
                }

                [[nodiscard]] uint8_t* data() noexcept { return data_; }
                [[nodiscard]] const uint8_t* data() const noexcept { return data_; }
                [[nodiscard]] size_t size() const noexcept { return size_; }
                [[nodiscard]] bool empty() const noexcept { return size_ == 0; }
                [[nodiscard]] uint8_t* begin() noexcept { return data_; }
                [[nodiscard]] uint8_t* end() noexcept { return data_ == nullptr ? nullptr : data_ + size_; }
                [[nodiscard]] const uint8_t* begin() const noexcept { return data_; }
                [[nodiscard]] const uint8_t* end() const noexcept { return data_ == nullptr ? nullptr : data_ + size_; }
                [[nodiscard]] uint8_t& operator[](size_t index) noexcept { return data_[index]; }
                [[nodiscard]] const uint8_t& operator[](size_t index) const noexcept { return data_[index]; }
                [[nodiscard]] operator std::span<const uint8_t>() const noexcept { return {data_, size_}; }

            private:
                void ensureArena() const { if (arena_ == nullptr) { throw std::logic_error("PackedTable: arena buffer is not bound"); } }

                BufferArena* arena_ = nullptr;
                uint8_t* data_ = nullptr;
                size_t size_ = 0;
                size_t capacity_ = 0;
        };

        public:
            using Entity = binpack::detail::classOf<PrimaryKeyPtr>;
            using PK = binpack::detail::memberOf<PrimaryKeyPtr>;
            using StableRowId = RowId;

            static_assert(!isImmutableField<PK>, "AkkaraDB Immutable: primary key fields cannot use akkaradb::Immutable<T>");

            struct Entry {
                PK id;
                Entity value;
            };

            PackedTable(PackedTable&&) noexcept = default;
            PackedTable& operator=(PackedTable&&) noexcept = default;
            PackedTable(const PackedTable&) = delete;
            PackedTable& operator=(const PackedTable&) = delete;

            template <auto FieldPtr>
            class Index;

    #include "detail/PTRefs.hpp"

    template <auto FieldPtr>
    [[nodiscard]] Index<FieldPtr> index() {
        static_assert(std::is_same_v < binpack::detail::classOf < FieldPtr >,
        Entity >, "index field must belong to the table entity"
        )
        ;
        const std::string_view fieldName = binpack::detail::memberName<FieldPtr>();
        const auto prefix = makeIndexPrefix(tableName_, fieldName);

        for (const auto& idx : indexes_) {
            if (idx.fieldName == fieldName) {
                return Index < FieldPtr >
                {
                    this, prefix
                };
            }
        }

        indexes_.push_back(
            IndexDef{
                prefix,
                std::string(fieldName),
                [](const Entity& entity, ArenaByteBuffer& out) {
                    out.clear();
                    encodeIndexFieldValue(entity.*FieldPtr, out);
                }
            }
        );
        return Index < FieldPtr >
        {
            this, prefix
        };
    }

    template <auto FieldPtr>
    PackedTable& indexed() {
        (void)index<FieldPtr>();
        return *this;
    }

    template <auto FieldPtr, typename Handler>
    PackedTable& onUpdate(Handler&& handler) {
        static_assert(std::is_same_v < binpack::detail::classOf < FieldPtr >,
        Entity >, "update field must belong to the table entity"
        )
        ;
        using Field = binpack::detail::memberOf<FieldPtr>;
        static_assert(
            requires(const Field& lhs, const Field& rhs) {
                { lhs == rhs } -> std::convertible_to<bool>;
            },
            "update field handlers require equality comparable fields"
        );

        updateFieldHooks_.push_back(
            UpdateFieldHookDef{
                std::string(binpack::detail::memberName<FieldPtr>()),
                [](const Entity& oldEntity, const Entity& newEntity) {
                    return static_cast<bool>((oldEntity.*FieldPtr) == (newEntity.*FieldPtr));
                },
                [callback = std::forward<Handler>(handler)](const Entity& oldEntity, Entity& newEntity) mutable {
                    invokeFieldUpdateHandler < FieldPtr > (callback, oldEntity, newEntity);
                }
            }
        );
        return *this;
    }

    #include "detail/PTForeignKey.hpp"
    #include "detail/PTActions.hpp"

    #include "detail/PTMutation.hpp"

    #include "detail/PTFind.hpp"

    #include "detail/PTScan.hpp"
    #include "detail/query/Plan.hpp"
    #include "detail/query/Join.hpp"

    [[nodiscard]] std::string_view tableName() const noexcept { return tableName_; }
    [[nodiscard]] engine::AkkEngine& engine() noexcept { return *engine_; }
    [[nodiscard]] const engine::AkkEngine& engine() const noexcept { return *engine_; }

    #include "detail/PTIndex.hpp"

    private
    :
    friend class AkkaraDB;
    template <auto>
    friend class PackedTable;

    #include "detail/PTDefs.hpp"

    engine::AkkEngine* engine_ = nullptr;
    std::string tableName_;
    std::array<uint8_t, 8> pkPrefix_{};
    std::array<uint8_t, 8> pkToRowIdPrefix_{};
    std::array<uint8_t, 8> rowIdToPkPrefix_{};
    std::array<uint8_t, 8> nextRowIdKey_{};
    std::vector<IndexDef> indexes_;
    std::vector<RefFieldDef> refFields_;
    std::vector<ForeignKeyDef> foreignKeys_;
    std::vector<CascadeDeleteDef> cascadeDeletes_;
    std::vector<RestrictDeleteDef> restrictDeletes_;
    std::vector<SetNullDeleteDef> setNullDeletes_;
    std::vector<UpdateCascadeDef> cascadeUpdates_;
    std::vector<UpdateRestrictDef> restrictUpdates_;
    std::vector<UpdateSetNullDef> setNullUpdates_;
    std::vector<UpdateFieldHookDef> updateFieldHooks_;
    std::vector<std::unique_ptr<RefBindingBase>> refBindings_;
    const RefBindingLookup* refBindingLookup_ = nullptr;
    bool updateActionsNeedTargetWrite_ = false;

    mutable std::unique_ptr<core::BufferArena> tempArena_ = std::make_unique<core::BufferArena>();
    mutable ArenaByteBuffer pkKeyBuffer_{tempArena_.get()};
    mutable ArenaByteBuffer valueBuffer_{tempArena_.get()};
    mutable ArenaByteBuffer indexKeyBuffer_{tempArena_.get()};
    mutable ArenaByteBuffer fieldBuffer_{tempArena_.get()};
    mutable ArenaByteBuffer scanStartBuffer_{tempArena_.get()};
    mutable ArenaByteBuffer scanEndBuffer_{tempArena_.get()};

    PackedTable() = default;

    void resetTempBuffers() const {
        tempArena_->reset();
        pkKeyBuffer_.bind(tempArena_.get());
        valueBuffer_.bind(tempArena_.get());
        indexKeyBuffer_.bind(tempArena_.get());
        fieldBuffer_.bind(tempArena_.get());
        scanStartBuffer_.bind(tempArena_.get());
        scanEndBuffer_.bind(tempArena_.get());
    }

    #include "detail/PTImmutable.hpp"

    template <auto LeftPtr, auto RightPtr>
    static consteval bool sameMemberPointer() {
        if constexpr (std::is_same_v<decltype(LeftPtr), decltype(RightPtr)>) { return LeftPtr == RightPtr; }
        else { return false; }
    }

    #include "detail/PTUpdateHooks.hpp"
    #include "detail/PTIndexedLookup.hpp"
    #include "detail/query/Optimize.hpp"
    #include "detail/PTCodec.hpp"
};

} // namespace akkaradb

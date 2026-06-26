/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkaradb/include/akkaradb/detail/PTDefs.hpp
#pragma once

struct IndexDef {
    std::array<uint8_t, 8> prefix;
    std::string fieldName;
    void (*encodeField)(const Entity&, ArenaByteBuffer&);
};

struct RefFieldDef {
    std::string fieldName;
    void* binding;
    void (*attach)(const Entity&, void*);
    void (*flush)(const Entity&, void*);
};

struct ForeignKeyDef {
    std::string fieldName;
    void* target;
    void (*validate)(const Entity&, void*);
};

struct CascadeDeleteDef {
    void* source;
    void* target;
    std::string fieldName;
    std::string targetFieldName;
    void (*cascade)(const Entity&, void*, void*);
};

struct RestrictDeleteDef {
    void* source;
    void* target;
    std::string fieldName;
    std::string targetFieldName;
    void (*restrictDelete)(const Entity&, void*, void*);
};

struct SetNullDeleteDef {
    void* source;
    void* target;
    std::string fieldName;
    std::string targetFieldName;
    void (*setNullDelete)(const Entity&, void*, void*);
};

struct UpdateCascadeDef {
    void* source;
    void* target;
    std::string fieldName;
    std::string targetFieldName;
    void (*cascadeUpdate)(const Entity&, const Entity&, void*, void*);
};

struct UpdateRestrictDef {
    void* source;
    void* target;
    std::string fieldName;
    std::string targetFieldName;
    void (*restrictUpdate)(const Entity&, const Entity&, void*, void*);
};

struct UpdateSetNullDef {
    void* source;
    void* target;
    std::string fieldName;
    std::string targetFieldName;
    void (*setNullUpdate)(const Entity&, const Entity&, void*, void*);
};

struct UpdateFieldHookDef {
    std::string fieldName;
    bool (*unchanged)(const Entity&, const Entity&);
    std::function<void(const Entity&, Entity&)> callback;
};

template <auto TargetPrimaryKeyPtr>
class TableRefBinding final : public RefBinding<binpack::detail::classOf<TargetPrimaryKeyPtr>> {
    public:
        using TargetEntity = binpack::detail::classOf<TargetPrimaryKeyPtr>;
        using Key = binpack::detail::memberOf<TargetPrimaryKeyPtr>;

        explicit TableRefBinding(PackedTable<TargetPrimaryKeyPtr>* table) : table_{table} {}

        [[nodiscard]] bool exists(const Key& key) const override { return table_->exists(key); }
        [[nodiscard]] std::optional<TargetEntity> get(const Key& key) const override { return table_->get(key); }
        [[nodiscard]] bool existsByRowId(RowId rowId) const override { return table_->primaryKeyOf(rowId).has_value(); }
        [[nodiscard]] std::optional<RowId> rowIdOf(const Key& key) const override { return table_->rowIdOf(key); }
        [[nodiscard]] std::optional<Key> keyOfRowId(RowId rowId) const override { return table_->primaryKeyOf(rowId); }
        [[nodiscard]] std::optional<TargetEntity> getByRowId(RowId rowId) const override { return table_->getByRowId(rowId); }
        void put(const TargetEntity& value) override { table_->put(value); }

    private:
        PackedTable<TargetPrimaryKeyPtr>* table_;
};

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

// akkaradb/include/akkaradb/AkkaraDB.hpp
#pragma once

#include "Export.hpp"
#include "PackedTable.hpp"
#include "akk/engine/AkkEngine.hpp"

#include <filesystem>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <typeindex>
#include <unordered_map>
#include <vector>

namespace akkaradb {
    enum class OnDelete {
        Cascade,
    };

    enum class StartupMode {
        ULTRA_FAST, FAST, NORMAL, DURABLE,
    };

    using Codec = engine::Codec;

    class AKDB_API AkkaraDB {
        public:
            class Schema;

            struct Options {
                std::filesystem::path dataDir;
                StartupMode mode = StartupMode::NORMAL;

                struct Overrides {
                    std::optional<size_t> memtableThresholdPerShard;
                    std::optional<bool> versionLogEnabled;
                    std::optional<engine::Codec> sstCodec;
                    std::optional<engine::Codec> blobCodec;
                    std::optional<uint64_t> blobThresholdBytes;
                    std::optional<bool> sstPromoteReads;
                    std::optional<size_t> sstBloomBitsPerKey;
                    std::optional<size_t> maxL0SstFiles;
                } overrides;
            };

            [[nodiscard]] static std::unique_ptr<AkkaraDB> open(std::filesystem::path dataDir, StartupMode mode = StartupMode::NORMAL);
            [[nodiscard]] static std::unique_ptr<AkkaraDB> open(Options options);

            ~AkkaraDB();

            AkkaraDB(const AkkaraDB&) = delete;
            AkkaraDB& operator=(const AkkaraDB&) = delete;
            AkkaraDB(AkkaraDB&&) = delete;
            AkkaraDB& operator=(AkkaraDB&&) = delete;

            void close();

            [[nodiscard]] engine::AkkEngine& engine() noexcept;
            [[nodiscard]] const engine::AkkEngine& engine() const noexcept;

            template <auto PrimaryKeyPtr>
            [[nodiscard]] PackedTable<PrimaryKeyPtr> table(std::string name) {
                using Table = PackedTable<PrimaryKeyPtr>;
                Table out;
                out.engine_ = &engine();
                out.tableName_ = std::move(name);
                out.pkPrefix_ = Table::makeTablePrefix(out.tableName_);
                return out;
            }

            [[nodiscard]] Schema schema();

        private:
            AkkaraDB() = default;

            #ifdef _MSC_VER
            #pragma warning(push)
            #pragma warning(disable: 4251)
            #endif
            std::unique_ptr<engine::AkkEngine> engine_;
            #ifdef _MSC_VER
            #pragma warning(pop)
            #endif
    };

    class AkkaraDB::Schema final : public RefBindingLookup {
        public:
            explicit Schema(AkkaraDB& db) : db_{db} {}

            Schema(Schema&& other) noexcept
                : db_{other.db_},
                  tables_{std::move(other.tables_)},
                  tablesByEntity_{std::move(other.tablesByEntity_)},
                  refBindingsByEntity_{std::move(other.refBindingsByEntity_)} {
                rebindRefs();
            }

            Schema& operator=(Schema&&) = delete;
            Schema(const Schema&) = delete;
            Schema& operator=(const Schema&) = delete;

            template <auto PrimaryKeyPtr>
            Schema& table(std::string name) {
                using Table = PackedTable<PrimaryKeyPtr>;
                using Entity = typename Table::Entity;
                auto holder = std::make_unique<TableHolder<PrimaryKeyPtr>>(db_.table<PrimaryKeyPtr>(std::move(name)));
                auto* raw = holder.get();
                tablesByEntity_[std::type_index(typeid(Entity))] = raw;
                refBindingsByEntity_[std::type_index(typeid(Entity))] = &raw->binding;
                tables_.push_back(std::move(holder));
                return *this;
            }

            template <auto FieldPtr>
            Schema& foreignKey(OnDelete onDelete = OnDelete::Cascade) {
                using Owner = binpack::detail::classOf<FieldPtr>;
                using Field = binpack::detail::memberOf<FieldPtr>;
                static_assert(isRef<Field>, "foreignKey field must be akkaradb::Ref<T>");
                using Target = typename RefTarget<Field>::Type;

                auto it = tablesByEntity_.find(std::type_index(typeid(Owner)));
                if (it == tablesByEntity_.end()) { throw std::runtime_error("AkkaraDB schema: foreign key owner table is not registered"); }

                auto* ownerHolder = dynamic_cast<TableHolder<RefTraits<Owner>::primaryKey>*>(it->second);
                if (ownerHolder == nullptr) { throw std::runtime_error("AkkaraDB schema: foreign key owner primary key binding mismatch"); }
                ownerHolder->table.template foreignKey<FieldPtr>();

                auto targetIt = tablesByEntity_.find(std::type_index(typeid(Target)));
                if (targetIt == tablesByEntity_.end()) { throw std::runtime_error("AkkaraDB schema: foreign key target table is not registered"); }
                auto* targetHolder = dynamic_cast<TableHolder<RefTraits<Target>::primaryKey>*>(targetIt->second);
                if (targetHolder == nullptr) { throw std::runtime_error("AkkaraDB schema: foreign key target primary key binding mismatch"); }

                if (onDelete == OnDelete::Cascade) { targetHolder->table.template cascadeDeleteFrom<FieldPtr>(ownerHolder->table); }
                return *this;
            }

            Schema open() {
                return std::move(*this);
            }

            template <typename Entity>
            [[nodiscard]] auto& table() {
                using Table = PackedTable<RefTraits<Entity>::primaryKey>;
                auto it = tablesByEntity_.find(std::type_index(typeid(Entity)));
                if (it == tablesByEntity_.end()) { throw std::runtime_error("AkkaraDB schema: table is not registered"); }
                auto* holder = dynamic_cast<TableHolder<RefTraits<Entity>::primaryKey>*>(it->second);
                if (holder == nullptr) { throw std::runtime_error("AkkaraDB schema: table primary key binding mismatch"); }
                return holder->table;
            }

            [[nodiscard]] RefBindingBase* findRefBinding(std::type_index entityType) const override {
                auto it = refBindingsByEntity_.find(entityType);
                if (it == refBindingsByEntity_.end()) { return nullptr; }
                return it->second;
            }

        private:
            struct TableHolderBase {
                virtual ~TableHolderBase() = default;
                virtual void bindRefsFrom(const RefBindingLookup& lookup) = 0;
            };

            template <auto PrimaryKeyPtr>
            struct TableHolder final : TableHolderBase {
                using Table = PackedTable<PrimaryKeyPtr>;
                using Entity = typename Table::Entity;
                using Key = typename Table::PK;

                explicit TableHolder(Table table) : table{std::move(table)}, binding{&this->table} {}

                void bindRefsFrom(const RefBindingLookup& lookup) override { table.bindRefsFrom(lookup); }

                struct Binding final : RefBinding<Entity> {
                    explicit Binding(Table* table) : table{table} {}

                    [[nodiscard]] bool exists(const Key& key) const override { return table->exists(key); }
                    [[nodiscard]] std::optional<Entity> get(const Key& key) const override { return table->get(key); }
                    void put(const Entity& value) override { table->put(value); }

                    Table* table;
                };

                Table table;
                Binding binding;
            };

            void rebindRefs() {
                for (const auto& table : tables_) { table->bindRefsFrom(*this); }
            }

            AkkaraDB& db_;
            std::vector<std::unique_ptr<TableHolderBase>> tables_;
            std::unordered_map<std::type_index, TableHolderBase*> tablesByEntity_;
            std::unordered_map<std::type_index, RefBindingBase*> refBindingsByEntity_;
    };

} // namespace akkaradb

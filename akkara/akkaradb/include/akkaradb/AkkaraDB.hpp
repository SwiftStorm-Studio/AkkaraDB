/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkaradb/include/akkaradb/AkkaraDB.hpp
#pragma once

#include "Export.hpp"
#include "PackedTable.hpp"
#include "akk/engine/AkkEngine.hpp"

#include <filesystem>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <typeindex>
#include <initializer_list>
#include <unordered_map>
#include <vector>

namespace akkaradb {
    enum class OnDelete : uint8_t {
        Cascade, Restrict, SetNull,
    };

    enum class OnUpdate : uint8_t {
        Cascade, Restrict, SetNull,
    };

    class OnDeleteOptions {
        public:
            constexpr OnDeleteOptions() = default;

            constexpr OnDeleteOptions(OnDelete action)
                : cascade_{action == OnDelete::Cascade}, restrict_{action == OnDelete::Restrict}, setNull_{action == OnDelete::SetNull} {}

            constexpr OnDeleteOptions(std::initializer_list<OnDelete> actions) {
                for (const auto action : actions) {
                    if (action == OnDelete::Cascade) { cascade_ = true; }
                    if (action == OnDelete::Restrict) { restrict_ = true; }
                    if (action == OnDelete::SetNull) { setNull_ = true; }
                }
            }

            [[nodiscard]] constexpr bool empty() const noexcept { return !cascade_ && !restrict_ && !setNull_; }
            [[nodiscard]] constexpr bool hasCascade() const noexcept { return cascade_; }
            [[nodiscard]] constexpr bool hasRestrict() const noexcept { return restrict_; }
            [[nodiscard]] constexpr bool hasSetNull() const noexcept { return setNull_; }
            [[nodiscard]] constexpr bool hasConflicts() const noexcept { return countSelected() > 1; }

        private:
            [[nodiscard]] constexpr uint8_t countSelected() const noexcept {
                return static_cast<uint8_t>(cascade_) + static_cast<uint8_t>(restrict_) + static_cast<uint8_t>(setNull_);
            }

            bool cascade_ = false;
            bool restrict_ = false;
            bool setNull_ = false;
    };

    class OnUpdateOptions {
        public:
            constexpr OnUpdateOptions() = default;

            constexpr OnUpdateOptions(OnUpdate action)
                : cascade_{action == OnUpdate::Cascade}, restrict_{action == OnUpdate::Restrict}, setNull_{action == OnUpdate::SetNull} {}

            constexpr OnUpdateOptions(std::initializer_list<OnUpdate> actions) {
                for (const auto action : actions) {
                    if (action == OnUpdate::Cascade) { cascade_ = true; }
                    if (action == OnUpdate::Restrict) { restrict_ = true; }
                    if (action == OnUpdate::SetNull) { setNull_ = true; }
                }
            }

            [[nodiscard]] constexpr bool empty() const noexcept { return !cascade_ && !restrict_ && !setNull_; }
            [[nodiscard]] constexpr bool hasCascade() const noexcept { return cascade_; }
            [[nodiscard]] constexpr bool hasRestrict() const noexcept { return restrict_; }
            [[nodiscard]] constexpr bool hasSetNull() const noexcept { return setNull_; }
            [[nodiscard]] constexpr bool hasConflicts() const noexcept { return countSelected() > 1; }

        private:
            [[nodiscard]] constexpr uint8_t countSelected() const noexcept {
                return static_cast<uint8_t>(cascade_) + static_cast<uint8_t>(restrict_) + static_cast<uint8_t>(setNull_);
            }

            bool cascade_ = false;
            bool restrict_ = false;
            bool setNull_ = false;
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

                struct ApiTlsOptions {
                    std::filesystem::path certPath;
                    std::filesystem::path keyPath;
                    std::filesystem::path caPath;
                    std::vector<uint8_t> psk;
                    std::string pskIdentity;
                    bool verifyPeer = true;
                };

                struct ApiOptions {
                    std::vector<engine::AkkEngineOptions::ApiBackend> backends;
                    std::filesystem::path serverBackendPath;
                    std::filesystem::path transportBackendPath;
                    std::filesystem::path httpBackendPath;
                    std::filesystem::path tcpBackendPath;
                    std::filesystem::path grpcBackendPath;
                    std::string bindHost = "127.0.0.1";
                    uint16_t httpPort = 7070;
                    uint16_t tcpPort = 7071;
                    uint16_t grpcPort = 7072;
                    engine::AkkEngineOptions::ApiTransportMode transportMode = engine::AkkEngineOptions::ApiTransportMode::PLAIN;
                    uint32_t httpMaxBatchItems = 4096;
                    uint32_t httpMaxScanItems = 4096;
                    uint32_t httpMaxHistoryEntries = 4096;
                    uint64_t httpMaxContentLength = 64ULL * 1024ULL * 1024ULL;
                    uint32_t grpcWorkerThreads = 0;
                    uint32_t grpcCompletionQueues = 0;
                    uint32_t grpcMinPollers = 0;
                    uint32_t grpcMaxPollers = 0;
                    uint32_t grpcMaxConcurrentStreams = 0;
                    uint64_t grpcResourceQuotaBytes = 0;
                    uint32_t grpcMaxBatchItems = 4096;
                    uint32_t grpcMaxScanItems = 4096;
                    uint32_t grpcMaxHistoryEntries = 4096;
                    engine::AkkEngineOptions::ApiIoBackend tcpIoBackend = engine::AkkEngineOptions::ApiIoBackend::AUTO;
                    uint32_t tcpWorkerThreads = 0;
                    uint32_t tcpAcceptQueueLimit = 4096;
                    uint32_t tcpAcceptQueueTimeoutMs = 60000;
                    uint32_t tcpListenBacklog = 1024;
                    uint32_t tcpRecvBufferBytes = 0;
                    uint32_t tcpSendBufferBytes = 0;
                    uint32_t tcpPipelineBatchLimit = 64;
                    uint32_t tcpMaxBatchItems = 4096;
                    uint64_t tcpMaxPendingResponseBytes = 8ULL * 1024ULL * 1024ULL;
                    uint32_t tcpReadTimeoutMs = 60000;
                    uint32_t tcpWriteTimeoutMs = 30000;
                    bool tcpNoDelay = true;
                    bool tcpKeepAlive = true;
                    ApiTlsOptions tls;
                };

                std::optional<ApiOptions> api;
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
                out.pkToRowIdPrefix_ = Table::makeMetaPrefix(out.tableName_, "pk2row");
                out.rowIdToPkPrefix_ = Table::makeMetaPrefix(out.tableName_, "row2pk");
                out.nextRowIdKey_ = Table::makeMetaPrefix(out.tableName_, "nextrow");
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
                  refBindingsByEntity_{std::move(other.refBindingsByEntity_)} { rebindRefs(); }

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
            Schema& foreignKey(OnDeleteOptions onDelete = {}, OnUpdateOptions onUpdate = {}) {
                using Field = binpack::detail::memberOf<FieldPtr>;
                static_assert(isRef<Field>, "foreignKey field must be akkaradb::Ref<T>");
                using Target = typename RefTarget<Field>::Type;

                return foreignKey<FieldPtr, RefTraits<Target>::primaryKey>(onDelete, onUpdate);
            }

            template <auto FieldPtr, auto TargetFieldPtr>
            Schema& foreignKey(OnDeleteOptions onDelete = {}, OnUpdateOptions onUpdate = {}) {
                using Owner = binpack::detail::classOf<FieldPtr>;
                using Field = binpack::detail::memberOf<FieldPtr>;
                using Target = binpack::detail::classOf<TargetFieldPtr>;

                if (onDelete.hasConflicts()) { throw std::invalid_argument("AkkaraDB schema: only one OnDelete action can be selected"); }
                if (onUpdate.hasConflicts()) { throw std::invalid_argument("AkkaraDB schema: only one OnUpdate action can be selected"); }

                auto it = tablesByEntity_.find(std::type_index(typeid(Owner)));
                if (it == tablesByEntity_.end()) { throw std::runtime_error("AkkaraDB schema: foreign key owner table is not registered"); }

                auto* ownerHolder = dynamic_cast<TableHolder<RefTraits<Owner>::primaryKey>*>(it->second);
                if (ownerHolder == nullptr) { throw std::runtime_error("AkkaraDB schema: foreign key owner primary key binding mismatch"); }

                auto targetIt = tablesByEntity_.find(std::type_index(typeid(Target)));
                if (targetIt == tablesByEntity_.end()) {
                    throw std::runtime_error("AkkaraDB schema: foreign key target table is not registered");
                }
                auto* targetHolder = dynamic_cast<TableHolder<RefTraits<Target>::primaryKey>*>(targetIt->second);
                if (targetHolder == nullptr) {
                    throw std::runtime_error("AkkaraDB schema: foreign key target primary key binding mismatch");
                }

                if constexpr (isRef<Field>) {
                    static_assert(
                        sameMemberPointer<TargetFieldPtr, RefTraits<Target>::primaryKey>(),
                        "AkkaraDB schema: Ref foreign keys currently require the target primary key"
                    );
                    ownerHolder->table.template foreignKey<FieldPtr>();
                }
                else {
                    ownerHolder->table.template foreignKey<FieldPtr, RefTraits<Target>::primaryKey, TargetFieldPtr>(targetHolder->table);
                }
                if (onDelete.hasRestrict()) {
                    targetHolder->table.template restrictDeleteFrom<FieldPtr, RefTraits<Owner>::primaryKey, TargetFieldPtr>(
                        ownerHolder->table
                    );
                }
                if (onDelete.hasSetNull()) {
                    targetHolder->table.template setNullDeleteFrom<FieldPtr, RefTraits<Owner>::primaryKey, TargetFieldPtr>(
                        ownerHolder->table
                    );
                }
                if (onDelete.hasCascade()) {
                    targetHolder->table.template cascadeDeleteFrom<FieldPtr, RefTraits<Owner>::primaryKey, TargetFieldPtr>(
                        ownerHolder->table
                    );
                }
                if (onUpdate.hasRestrict()) {
                    targetHolder->table.template restrictUpdateFrom<FieldPtr, RefTraits<Owner>::primaryKey, TargetFieldPtr>(
                        ownerHolder->table
                    );
                }
                if (onUpdate.hasSetNull()) {
                    targetHolder->table.template setNullUpdateFrom<FieldPtr, RefTraits<Owner>::primaryKey, TargetFieldPtr>(
                        ownerHolder->table
                    );
                }
                if (onUpdate.hasCascade()) {
                    targetHolder->table.template cascadeUpdateFrom<FieldPtr, RefTraits<Owner>::primaryKey, TargetFieldPtr>(
                        ownerHolder->table
                    );
                }
                return *this;
            }

            Schema open() { return std::move(*this); }

            template <typename Entity>
            [[nodiscard]] auto& table() {
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
                    [[nodiscard]] bool existsByRowId(RowId rowId) const override { return table->primaryKeyOf(rowId).has_value(); }
                    [[nodiscard]] std::optional<RowId> rowIdOf(const Key& key) const override { return table->rowIdOf(key); }
                    [[nodiscard]] std::optional<Key> keyOfRowId(RowId rowId) const override { return table->primaryKeyOf(rowId); }
                    [[nodiscard]] std::optional<Entity> getByRowId(RowId rowId) const override { return table->getByRowId(rowId); }
                    void put(const Entity& value) override { table->put(value); }

                    Table* table;
                };

                Table table;
                Binding binding;
            };

            void rebindRefs() { for (const auto& table : tables_) { table->bindRefsFrom(*this); } }

            template <auto LeftPtr, auto RightPtr>
            static consteval bool sameMemberPointer() {
                if constexpr (std::is_same_v<decltype(LeftPtr), decltype(RightPtr)>) { return LeftPtr == RightPtr; }
                else { return false; }
            }

            AkkaraDB& db_;
            std::vector<std::unique_ptr<TableHolderBase>> tables_;
            std::unordered_map<std::type_index, TableHolderBase*> tablesByEntity_;
            std::unordered_map<std::type_index, RefBindingBase*> refBindingsByEntity_;
    };
} // namespace akkaradb

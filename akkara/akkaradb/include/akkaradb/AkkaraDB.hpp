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
#include <string>

namespace akkaradb {
    enum class StartupMode {
        ULTRA_FAST, FAST, NORMAL, DURABLE,
    };

    using Codec = engine::Codec;

    class AKDB_API AkkaraDB {
        public:
            struct Options {
                std::filesystem::path data_dir;
                StartupMode mode = StartupMode::NORMAL;

                struct Overrides {
                    std::optional<size_t> memtable_threshold_per_shard;
                    std::optional<bool> version_log_enabled;
                    std::optional<engine::Codec> sst_codec;
                    std::optional<engine::Codec> blob_codec;
                    std::optional<uint64_t> blob_threshold_bytes;
                    std::optional<bool> sst_promote_reads;
                    std::optional<size_t> sst_bloom_bits_per_key;
                    std::optional<size_t> max_l0_sst_files;
                } overrides;
            };

            [[nodiscard]] static std::unique_ptr<AkkaraDB> open(std::filesystem::path data_dir, StartupMode mode = StartupMode::NORMAL);
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
                out.table_name_ = std::move(name);
                out.pk_prefix_ = Table::make_table_prefix(out.table_name_);
                return out;
            }

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
} // namespace akkaradb

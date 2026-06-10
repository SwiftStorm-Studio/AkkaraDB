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

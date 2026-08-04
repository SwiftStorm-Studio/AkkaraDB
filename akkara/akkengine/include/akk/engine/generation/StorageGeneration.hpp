/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/generation/StorageGeneration.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include <filesystem>
#include <string>

namespace akkaradb::engine::generation {
    /** Active/staging generation directory manager for a database root. */
    class AKDB_API StorageGeneration {
        public:
            /** Opens the active generation, creating `generations/gen-1` for an empty root. */
            [[nodiscard]] static StorageGeneration openOrCreate(const std::filesystem::path& databaseRoot);

            /** Creates an empty staging generation. It is not visible to readers. */
            [[nodiscard]] std::filesystem::path createStaging(const std::string& generationId) const;

            /** Makes a validated staging generation active by replacing the current pointer. */
            void activate(const std::string& generationId) const;

            [[nodiscard]] const std::filesystem::path& root() const noexcept { return root_; }
            [[nodiscard]] const std::filesystem::path& activePath() const noexcept { return activePath_; }
            [[nodiscard]] std::filesystem::path generationsPath() const { return root_ / "generations"; }
            [[nodiscard]] std::filesystem::path stagingPath() const { return root_ / "staging"; }

        private:
            StorageGeneration(std::filesystem::path root, std::filesystem::path activePath);
            std::filesystem::path root_;
            std::filesystem::path activePath_;
    };
} // namespace akkaradb::engine::generation

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/generation/StorageGeneration.cpp
#include "akk/engine/generation/StorageGeneration.hpp"

#include <fstream>
#include <stdexcept>

namespace akkaradb::engine::generation {
    namespace fs = std::filesystem;

    namespace {
        constexpr const char* CURRENT_FILE = "current.akgen";

        void writeCurrent(const fs::path& root, const std::string& id) {
            const fs::path current = root / CURRENT_FILE;
            const fs::path temporary = root / (std::string{CURRENT_FILE} + ".tmp");
            {
                std::ofstream out(temporary, std::ios::binary | std::ios::trunc);
                if (!out) { throw std::runtime_error("StorageGeneration: cannot create current pointer"); }
                out << id << '\n';
                out.flush();
                if (!out) { throw std::runtime_error("StorageGeneration: cannot write current pointer"); }
            }
            std::error_code ec;
            fs::rename(temporary, current, ec);
            if (ec) {
                // Windows does not replace an existing destination with rename().
                fs::remove(current, ec);
                ec.clear();
                fs::rename(temporary, current, ec);
            }
            if (ec) { throw std::runtime_error("StorageGeneration: cannot activate generation: " + ec.message()); }
        }

        std::string readCurrent(const fs::path& root) {
            std::ifstream in(root / CURRENT_FILE, std::ios::binary);
            std::string id;
            std::getline(in, id);
            if (!in || id.empty() || id.find_first_of("/\\\\") != std::string::npos || id == "." || id == "..") {
                throw std::runtime_error("StorageGeneration: invalid current generation pointer");
            }
            return id;
        }
    } // namespace

    StorageGeneration::StorageGeneration(fs::path root, fs::path activePath) : root_{std::move(root)}, activePath_{std::move(activePath)} {}

    StorageGeneration StorageGeneration::openOrCreate(const fs::path& databaseRoot) {
        if (databaseRoot.empty()) { throw std::invalid_argument("StorageGeneration: database root is required"); }
        fs::create_directories(databaseRoot);
        const fs::path current = databaseRoot / CURRENT_FILE;
        if (fs::exists(current)) {
            const auto id = readCurrent(databaseRoot);
            const fs::path active = databaseRoot / "generations" / id;
            if (!fs::is_directory(active)) { throw std::runtime_error("StorageGeneration: active generation directory is missing"); }
            return StorageGeneration{databaseRoot, active};
        }
        for (const auto& item : fs::directory_iterator(databaseRoot)) {
            const auto name = item.path().filename().string();
            if (name != "generations" && name != "staging") {
                throw std::runtime_error("StorageGeneration: legacy data directory requires explicit generation migration");
            }
        }
        const std::string id = "gen-1";
        const fs::path active = databaseRoot / "generations" / id;
        fs::create_directories(active);
        fs::create_directories(databaseRoot / "staging");
        writeCurrent(databaseRoot, id);
        return StorageGeneration{databaseRoot, active};
    }

    fs::path StorageGeneration::createStaging(const std::string& generationId) const {
        if (generationId.empty() || generationId.find_first_of("/\\\\") != std::string::npos) {
            throw std::invalid_argument("StorageGeneration: invalid generation id");
        }
        const fs::path staging = stagingPath() / generationId;
        if (fs::exists(staging)) { throw std::runtime_error("StorageGeneration: staging generation already exists"); }
        fs::create_directories(staging);
        return staging;
    }

    void StorageGeneration::activate(const std::string& generationId) const {
        const fs::path staged = stagingPath() / generationId;
        const fs::path target = generationsPath() / generationId;
        if (!fs::is_directory(staged)) { throw std::runtime_error("StorageGeneration: staging generation is missing"); }
        if (fs::exists(target)) { throw std::runtime_error("StorageGeneration: target generation already exists"); }
        fs::create_directories(generationsPath());
        fs::rename(staged, target);
        writeCurrent(root_, generationId);
    }
} // namespace akkaradb::engine::generation

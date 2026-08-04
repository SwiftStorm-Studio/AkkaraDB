/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/sstable/SSTWriter.hpp
/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/sstable/SSTWriter.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include <cstdint>
#include <filesystem>
#include <optional>
#include <span>
#include <vector>

#include "akk/core/record/RecordView.hpp"
#include "akk/core/utils/ArenaGenerator.hpp"
#include "akk/engine/sstable/SSTFormat.hpp"

namespace akkaradb::engine::sst {
    class AKDB_API SSTWriter {
        public:
            enum class Codec : uint8_t {
                NONE = 0, ZSTD = 1,
            };

            struct Options {
                int level = 0;
                uint32_t blockSize = SST_DEFAULT_BLOCK_SIZE;
                uint64_t targetFileSize = SST_DEFAULT_TARGET_FILE_SIZE;
                uint32_t bloomBitsPerKey = SST_DEFAULT_BLOOM_BITS_PER_KEY;
                Codec codec = Codec::ZSTD;
                int zstdCompressionLevel = 1;
            };

            struct Result {
                struct BlobRefEntry {
                    std::vector<uint8_t> key;
                    uint64_t seq = 0;
                    uint8_t flags = 0;
                    std::optional<uint64_t> blobId;
                };

                std::filesystem::path path;
                uint64_t entryCount = 0;
                uint64_t fileSizeBytes = 0;
                uint64_t minSeq = UINT64_MAX;
                uint64_t maxSeq = 0;
                std::vector<uint8_t> firstKey;
                std::vector<uint8_t> lastKey;
                std::vector<BlobRefEntry> blobRefs;
            };

            [[nodiscard]] static Result write(const std::filesystem::path& path, std::span<const core::RecordView> records);
            [[nodiscard]] static Result write(
                const std::filesystem::path& path,
                std::span<const core::RecordView> records,
                const Options& options
            );
            [[nodiscard]] static Result write(
                const std::filesystem::path& path,
                size_t estimatedRecordCount,
                core::ArenaGenerator<core::RecordView> records,
                const Options& options
            );

        private:
            SSTWriter() = delete;
    };
} // namespace akkaradb::engine::sst

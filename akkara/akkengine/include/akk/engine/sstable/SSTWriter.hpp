

// akkengine/include/akk/engine/sstable/SSTWriter.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include <cstdint>
#include <filesystem>
#include <span>
#include <vector>

#include "akk/core/record/RecordView.hpp"
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
            };

            struct Result {
                std::filesystem::path path;
                uint64_t entryCount = 0;
                uint64_t fileSizeBytes = 0;
                uint64_t minSeq = UINT64_MAX;
                uint64_t maxSeq = 0;
                std::vector<uint8_t> firstKey;
                std::vector<uint8_t> lastKey;
            };

            [[nodiscard]] static Result write(const std::filesystem::path& path, std::span<const core::RecordView> records);
            [[nodiscard]] static Result write(
                const std::filesystem::path& path,
                std::span<const core::RecordView> records,
                const Options& options
            );

        private:
            SSTWriter() = delete;
    };
} // namespace akkaradb::engine::sst

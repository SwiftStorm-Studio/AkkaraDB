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

// akkaradb/src/AkkaraDB.cpp
#include "akkaradb/AkkaraDB.hpp"

#include "akk/engine/wal/WalWriter.hpp"

#include <stdexcept>
#include <utility>

namespace akkaradb {
    namespace {
        [[nodiscard]] engine::sst::SSTWriter::Codec to_sst_codec(engine::Codec codec) noexcept {
            switch (codec) {
                case engine::Codec::None: return engine::sst::SSTWriter::Codec::None;
                case engine::Codec::Zstd: return engine::sst::SSTWriter::Codec::Zstd;
            }
            return engine::sst::SSTWriter::Codec::None;
        }

        [[nodiscard]] engine::blob::BlobCodec to_blob_codec(engine::Codec codec) noexcept {
            switch (codec) {
                case engine::Codec::None: return engine::blob::BlobCodec::None;
                case engine::Codec::Zstd: return engine::blob::BlobCodec::Zstd;
            }
            return engine::blob::BlobCodec::None;
        }

        [[nodiscard]] engine::AkkEngineOptions make_engine_options(AkkaraDB::Options options) {
            engine::AkkEngineOptions out;
            out.paths.data_dir = std::move(options.data_dir);

            switch (options.mode) {
                case StartupMode::ULTRA_FAST: out.components.wal_enabled = false;
                    out.components.blob_enabled = false;
                    out.components.manifest_enabled = false;
                    out.components.sst_enabled = false;
                    out.components.version_log_enabled = false;
                    out.runtime.force_flush_on_close = false;
                    out.runtime.force_sync_on_close = false;
                    out.memtable.threshold_bytes_per_shard = 512ULL * 1024ULL * 1024ULL;
                    break;
                case StartupMode::FAST: out.wal.sync_mode = engine::wal::WalSyncMode::Async;
                    out.components.version_log_enabled = false;
                    out.runtime.sst_promote_reads = true;
                    out.memtable.threshold_bytes_per_shard = 256ULL * 1024ULL * 1024ULL;
                    break;
                case StartupMode::NORMAL: out.wal.sync_mode = engine::wal::WalSyncMode::Async;
                    break;
                case StartupMode::DURABLE: out.wal.sync_mode = engine::wal::WalSyncMode::Sync;
                    out.components.version_log_enabled = true;
                    break;
            }

            if (options.overrides.memtable_threshold_per_shard) { out.memtable.threshold_bytes_per_shard = *options.overrides.memtable_threshold_per_shard; }
            if (options.overrides.version_log_enabled) { out.components.version_log_enabled = *options.overrides.version_log_enabled; }
            if (options.overrides.sst_codec) { out.sst.codec = to_sst_codec(*options.overrides.sst_codec); }
            if (options.overrides.blob_codec) { out.blob.codec = to_blob_codec(*options.overrides.blob_codec); }
            if (options.overrides.blob_threshold_bytes) { out.blob.threshold_bytes = *options.overrides.blob_threshold_bytes; }
            if (options.overrides.sst_promote_reads) { out.runtime.sst_promote_reads = *options.overrides.sst_promote_reads; }
            if (options.overrides.sst_bloom_bits_per_key) { out.sst.bloom_bits_per_key = static_cast<uint32_t>(*options.overrides.sst_bloom_bits_per_key); }
            if (options.overrides.max_l0_sst_files) { out.sst.max_l0_files = static_cast<int>(*options.overrides.max_l0_sst_files); }

            return out;
        }
    } // namespace

    std::unique_ptr<AkkaraDB> AkkaraDB::open(std::filesystem::path data_dir, StartupMode mode) {
        Options options;
        options.data_dir = std::move(data_dir);
        options.mode = mode;
        return open(std::move(options));
    }

    std::unique_ptr<AkkaraDB> AkkaraDB::open(Options options) {
        auto db = std::unique_ptr<AkkaraDB>{new AkkaraDB()};
        db->engine_ = engine::AkkEngine::open(make_engine_options(std::move(options)));
        return db;
    }

    AkkaraDB::~AkkaraDB() { close(); }

    void AkkaraDB::close() { if (engine_) { engine_->close(); } }

    engine::AkkEngine& AkkaraDB::engine() noexcept { return *engine_; }
    const engine::AkkEngine& AkkaraDB::engine() const noexcept { return *engine_; }
} // namespace akkaradb

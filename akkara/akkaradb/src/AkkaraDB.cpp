/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkaradb/src/AkkaraDB.cpp
#include "akkaradb/AkkaraDB.hpp"

#include "akk/engine/wal/WalWriter.hpp"

#include <stdexcept>
#include <utility>

namespace akkaradb {
    namespace {
        [[nodiscard]] engine::sst::SSTWriter::Codec toSstCodec(engine::Codec codec) noexcept {
            switch (codec) {
                case engine::Codec::NONE: return engine::sst::SSTWriter::Codec::NONE;
                case engine::Codec::ZSTD: return engine::sst::SSTWriter::Codec::ZSTD;
            }
            return engine::sst::SSTWriter::Codec::NONE;
        }

        [[nodiscard]] engine::blob::BlobCodec toBlobCodec(engine::Codec codec) noexcept {
            switch (codec) {
                case engine::Codec::NONE: return engine::blob::BlobCodec::NONE;
                case engine::Codec::ZSTD: return engine::blob::BlobCodec::ZSTD;
            }
            return engine::blob::BlobCodec::NONE;
        }

        [[nodiscard]] engine::AkkEngineOptions makeEngineOptions(AkkaraDB::Options options) {
            engine::AkkEngineOptions out;
            out.paths.dataDir = std::move(options.dataDir);

            switch (options.mode) {
                case StartupMode::ULTRA_FAST: out.components.walEnabled = false;
                    out.components.blobEnabled = false;
                    out.components.manifestEnabled = false;
                    out.components.sstEnabled = false;
                    out.components.versionLogEnabled = false;
                    out.runtime.forceFlushOnClose = false;
                    out.runtime.forceSyncOnClose = false;
                    out.memtable.thresholdBytesPerShard = 512ULL * 1024ULL * 1024ULL;
                    break;
                case StartupMode::FAST: out.wal.syncMode = engine::wal::WalSyncMode::ASYNC;
                    out.components.versionLogEnabled = false;
                    out.runtime.sstPromoteReads = true;
                    out.memtable.thresholdBytesPerShard = 256ULL * 1024ULL * 1024ULL;
                    break;
                case StartupMode::NORMAL: out.wal.syncMode = engine::wal::WalSyncMode::ASYNC;
                    break;
                case StartupMode::DURABLE: out.wal.syncMode = engine::wal::WalSyncMode::SYNC;
                    out.components.versionLogEnabled = true;
                    break;
            }

            if (options.overrides.memtableThresholdPerShard) {
                out.memtable.thresholdBytesPerShard = *options.overrides.memtableThresholdPerShard;
            }
            if (options.overrides.versionLogEnabled) { out.components.versionLogEnabled = *options.overrides.versionLogEnabled; }
            if (options.overrides.sstCodec) { out.sst.codec = toSstCodec(*options.overrides.sstCodec); }
            if (options.overrides.blobCodec) { out.blob.codec = toBlobCodec(*options.overrides.blobCodec); }
            if (options.overrides.blobThresholdBytes) { out.blob.thresholdBytes = *options.overrides.blobThresholdBytes; }
            if (options.overrides.sstPromoteReads) { out.runtime.sstPromoteReads = *options.overrides.sstPromoteReads; }
            if (options.overrides.sstBloomBitsPerKey) {
                out.sst.bloomBitsPerKey = static_cast<uint32_t>(*options.overrides.sstBloomBitsPerKey);
            }
            if (options.overrides.maxL0SstFiles) { out.sst.maxL0Files = static_cast<int>(*options.overrides.maxL0SstFiles); }

            return out;
        }
    } // namespace

    std::unique_ptr<AkkaraDB> AkkaraDB::open(std::filesystem::path dataDir, StartupMode mode) {
        Options options;
        options.dataDir = std::move(dataDir);
        options.mode = mode;
        return open(std::move(options));
    }

    std::unique_ptr<AkkaraDB> AkkaraDB::open(Options options) {
        auto db = std::unique_ptr<AkkaraDB>{new AkkaraDB()};
        db->engine_ = engine::AkkEngine::open(makeEngineOptions(std::move(options)));
        return db;
    }

    AkkaraDB::~AkkaraDB() { close(); }

    void AkkaraDB::close() { if (engine_) { engine_->close(); } }

    engine::AkkEngine& AkkaraDB::engine() noexcept { return *engine_; }
    const engine::AkkEngine& AkkaraDB::engine() const noexcept { return *engine_; }

    AkkaraDB::Schema AkkaraDB::schema() { return Schema{*this}; }
} // namespace akkaradb

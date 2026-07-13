/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/include/akk/engine/AkkEngine.hpp
#pragma once

#include "akkaradb/Export.hpp"

#include "akkaradb/Stats.hpp"
#include "akk/engine/blob/BlobManager.hpp"
#include "akk/engine/cluster/ClusterConfig.hpp"
#include "akk/engine/memtable/MemTable.hpp"
#include "akk/engine/sstable/SSTManager.hpp"
#include "akk/engine/vlog/VersionLog.hpp"
#include "akk/engine/wal/WalWriter.hpp"
#include "akk/core/buffer/BufferArena.hpp"
#include "akk/core/utils/ArenaGenerator.hpp"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace akkaradb::engine {
    using VersionEntry = vlog::VersionEntry;

    enum class Codec : uint8_t {
        NONE = 0, ZSTD = 1,
    };

    struct AKDB_API AkkEngineOptions {
        struct Paths {
            std::filesystem::path dataDir;
            std::filesystem::path walDir;
            std::filesystem::path blobDir;
            std::filesystem::path sstDir;
            std::filesystem::path manifestPath;
            std::filesystem::path versionLogPath;
            std::filesystem::path clusterConfigPath;
            std::filesystem::path nodeIdPath;
        } paths;

        struct Components {
            bool walEnabled = true;
            bool blobEnabled = true;
            bool manifestEnabled = true;
            bool sstEnabled = true;
            bool versionLogEnabled = false;
            bool clusterEnabled = false;
            bool apiEnabled = false;
        } components;

        struct ManifestOptions {
            bool fastMode = false;
        } manifest;

        struct ClusterOptions {
            std::optional<cluster::ClusterConfig> config;
            std::filesystem::path runtimeBackendPath;
            cluster::ClusterRuntimeOptions runtime;
        } cluster;

        enum class ApiBackend : uint8_t {
            HTTP = 0, TCP = 1, GRPC = 2,
        };

        enum class ApiIoBackend : uint8_t {
            AUTO = 0, THREAD_POOL = 1,
        };

        enum class ApiTransportMode : uint8_t {
            TLS = 0, PLAIN = 1,
        };

        struct ApiTlsOptions {
            std::filesystem::path certPath;
            std::filesystem::path keyPath;
            std::filesystem::path caPath;
            std::vector<uint8_t> psk;
            std::string pskIdentity;
            bool verifyPeer = true;
        };

        struct ApiOptions {
            std::vector<ApiBackend> backends;
            std::filesystem::path serverBackendPath;
            std::filesystem::path transportBackendPath;
            std::filesystem::path httpBackendPath;
            std::filesystem::path tcpBackendPath;
            std::filesystem::path grpcBackendPath;
            std::string bindHost;
            uint16_t httpPort = 7070;
            uint32_t httpMaxBatchItems = 4096;
            uint32_t httpMaxScanItems = 4096;
            uint32_t httpMaxHistoryEntries = 4096;
            uint64_t httpMaxContentLength = 64ULL * 1024ULL * 1024ULL;
            uint16_t tcpPort = 7071;
            uint16_t grpcPort = 7072;
            uint32_t grpcWorkerThreads = 0;
            uint32_t grpcCompletionQueues = 0;
            uint32_t grpcMinPollers = 0;
            uint32_t grpcMaxPollers = 0;
            uint32_t grpcMaxConcurrentStreams = 0;
            uint64_t grpcResourceQuotaBytes = 0;
            uint32_t grpcMaxBatchItems = 4096;
            uint32_t grpcMaxScanItems = 4096;
            uint32_t grpcMaxHistoryEntries = 4096;
            ApiIoBackend tcpIoBackend = ApiIoBackend::AUTO;
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
            ApiTransportMode transportMode = ApiTransportMode::TLS;
            ApiTlsOptions tls;
        } api;

        struct RuntimeOptions {
            uint32_t writerThreads = 0;
            bool recoverWal = true;
            bool recoverSst = true;
            bool pruneWalOnFlush = true;
            bool forceFlushOnClose = true;
            bool forceSyncOnClose = true;
            bool sstPromoteReads = false;
            // Store mutable engine files under an active generation directory.
            // Disabled by default so existing data directories retain their layout.
            bool generationLayoutEnabled = false;
        } runtime;

        memtable::MemTable::Options memtable;
        wal::WalOptions wal;
        blob::BlobManager::Options blob;
        sst::SSTManager::Options sst;
        vlog::VersionLogOptions vlog;
    };

    class AKDB_API AkkEngine {
        public:
            struct ScanRecordView {
                std::span<const uint8_t> key;
                std::span<const uint8_t> value;
            };

            struct BatchPutEntry {
                std::span<const uint8_t> key;
                std::span<const uint8_t> value;
            };

            struct BatchGetResult {
                bool found = false;
                std::vector<uint8_t> value;
            };

            [[nodiscard]] static std::unique_ptr<AkkEngine> open(AkkEngineOptions options);
            ~AkkEngine();

            AkkEngine(const AkkEngine&) = delete;
            AkkEngine& operator=(const AkkEngine&) = delete;
            AkkEngine(AkkEngine&&) = delete;
            AkkEngine& operator=(AkkEngine&&) = delete;

            void put(std::span<const uint8_t> key, std::span<const uint8_t> value);
            void putHinted(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t fp64, uint64_t miniKey);
            void putBatch(std::span<const BatchPutEntry> entries);
            void remove(std::span<const uint8_t> key);
            void removeHinted(std::span<const uint8_t> key, uint64_t fp64, uint64_t miniKey);

            [[nodiscard]] std::optional<std::vector<uint8_t>> get(std::span<const uint8_t> key) const;
            [[nodiscard]] std::vector<BatchGetResult> getBatch(std::span<const std::span<const uint8_t>> keys) const;
            [[nodiscard]] bool exists(std::span<const uint8_t> key) const;
            [[nodiscard]] bool getInto(std::span<const uint8_t> key, std::vector<uint8_t>& out) const;
            [[nodiscard]] bool getIntoArena(std::span<const uint8_t> key, core::BufferArena& arena, std::span<const uint8_t>& out) const;
            [[nodiscard]] size_t count(std::span<const uint8_t> startKey = {}, std::span<const uint8_t> endKey = {}) const;
            [[nodiscard]] core::ArenaGenerator<ScanRecordView> scan(
                core::BufferArena& arena,
                std::span<const uint8_t> startKey = {},
                std::span<const uint8_t> endKey = {}
            ) const;

            [[nodiscard]] std::optional<std::vector<uint8_t>> getAt(std::span<const uint8_t> key, uint64_t atSeq) const;
            [[nodiscard]] std::vector<VersionEntry> history(std::span<const uint8_t> key) const;
            void rollbackTo(uint64_t targetSeq);
            void rollbackKey(std::span<const uint8_t> key, uint64_t targetSeq);

            [[nodiscard]] EngineStats stats() const noexcept;

            void forceSync();
            void forceFlush();
            void runBlobGc();
            void close();

        private:
            AkkEngine();

            class Impl;
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine

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

// akkengine/include/akk/engine/AkkEngine.hpp
#pragma once

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
        None = 0, Zstd = 1,
    };

    struct AkkEngineOptions {
        struct Paths {
            std::filesystem::path data_dir;
            std::filesystem::path wal_dir;
            std::filesystem::path blob_dir;
            std::filesystem::path sst_dir;
            std::filesystem::path manifest_path;
            std::filesystem::path version_log_path;
            std::filesystem::path cluster_config_path;
            std::filesystem::path node_id_path;
        } paths;

        struct Components {
            bool wal_enabled = true;
            bool blob_enabled = true;
            bool manifest_enabled = true;
            bool sst_enabled = true;
            bool version_log_enabled = false;
            bool cluster_enabled = false;
            bool api_enabled = false;
        } components;

        struct ManifestOptions {
            bool fast_mode = false;
        } manifest;

        struct ClusterOptions {
            std::optional<cluster::ClusterConfig> config;
            cluster::ClusterRuntimeOptions runtime;
        } cluster;

        enum class ApiBackend : uint8_t {
            Http = 0, Tcp = 1,
        };

        enum class ApiIoBackend : uint8_t {
            Auto = 0, ThreadPool = 1,
        };

        struct ApiTlsOptions {
            std::filesystem::path cert_path;
            std::filesystem::path key_path;
            std::filesystem::path ca_path;
            std::vector<uint8_t> psk;
            std::string psk_identity;
            bool verify_peer = true;
        };

        struct ApiOptions {
            std::vector<ApiBackend> backends;
            std::string bind_host;
            uint16_t http_port = 7070;
            uint16_t tcp_port = 7071;
            ApiIoBackend tcp_io_backend = ApiIoBackend::Auto;
            uint32_t tcp_worker_threads = 0;
            uint32_t tcp_accept_queue_limit = 4096;
            uint32_t tcp_accept_queue_timeout_ms = 60000;
            uint32_t tcp_listen_backlog = 1024;
            uint32_t tcp_recv_buffer_bytes = 0;
            uint32_t tcp_send_buffer_bytes = 0;
            uint32_t tcp_pipeline_batch_limit = 64;
            uint32_t tcp_max_batch_items = 4096;
            uint64_t tcp_max_pending_response_bytes = 8ULL * 1024ULL * 1024ULL;
            uint32_t tcp_read_timeout_ms = 60000;
            uint32_t tcp_write_timeout_ms = 30000;
            bool tcp_no_delay = true;
            bool tcp_keep_alive = true;
            cluster::TransportMode transport_mode = cluster::TransportMode::TLS;
            ApiTlsOptions tls;
        } api;

        struct RuntimeOptions {
            uint32_t writer_threads = 0;
            bool recover_wal = true;
            bool recover_sst = true;
            bool prune_wal_on_flush = true;
            bool force_flush_on_close = true;
            bool force_sync_on_close = true;
            bool sst_promote_reads = false;
        } runtime;

        memtable::MemTable::Options memtable;
        wal::WalOptions wal;
        blob::BlobManager::Options blob;
        sst::SSTManager::Options sst;
        vlog::VersionLogOptions vlog;
    };

    class AkkEngine {
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
            void put_hinted(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t fp64, uint64_t mini_key);
            void put_batch(std::span<const BatchPutEntry> entries);
            void remove(std::span<const uint8_t> key);
            void remove_hinted(std::span<const uint8_t> key, uint64_t fp64, uint64_t mini_key);

            [[nodiscard]] std::optional<std::vector<uint8_t>> get(std::span<const uint8_t> key) const;
            [[nodiscard]] std::vector<BatchGetResult> get_batch(std::span<const std::span<const uint8_t>> keys) const;
            [[nodiscard]] bool exists(std::span<const uint8_t> key) const;
            [[nodiscard]] bool get_into(std::span<const uint8_t> key, std::vector<uint8_t>& out) const;
            [[nodiscard]] bool get_into_arena(std::span<const uint8_t> key, core::BufferArena& arena, std::span<const uint8_t>& out) const;
            [[nodiscard]] size_t count(std::span<const uint8_t> start_key = {}, std::span<const uint8_t> end_key = {}) const;
            [[nodiscard]] core::ArenaGenerator<ScanRecordView> scan(
                core::BufferArena& arena,
                std::span<const uint8_t> start_key = {},
                std::span<const uint8_t> end_key = {}
            ) const;

            [[nodiscard]] std::optional<std::vector<uint8_t>> get_at(std::span<const uint8_t> key, uint64_t at_seq) const;
            [[nodiscard]] std::vector<VersionEntry> history(std::span<const uint8_t> key) const;
            void rollback_to(uint64_t target_seq);
            void rollback_key(std::span<const uint8_t> key, uint64_t target_seq);

            [[nodiscard]] EngineStats stats() const noexcept;

            void force_sync();
            void force_flush();
            void close();

        private:
            AkkEngine();

            class Impl;
            std::unique_ptr<Impl> impl_;
    };
} // namespace akkaradb::engine

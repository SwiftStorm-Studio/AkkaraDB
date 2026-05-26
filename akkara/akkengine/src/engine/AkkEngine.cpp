/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 */

// akkengine/src/engine/AkkEngine.cpp
#include "akk/engine/AkkEngine.hpp"

#include "akk/core/record/KeyFingerprint.hpp"
#include "akk/core/record/MemHdr16.hpp"
#include "akk/engine/blob/BlobFraming.hpp"
#include "akk/engine/cluster/ClusterRuntime.hpp"
#include "akk/engine/manifest/Manifest.hpp"
#include "akk/engine/server/AkkApiServer.hpp"
#include "akk/engine/wal/WalRecovery.hpp"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>

namespace akkaradb::engine {
    namespace fs = std::filesystem;

    namespace {
        [[nodiscard]] uint64_t now_ns() noexcept {
            timespec ts{};
            timespec_get(&ts, TIME_UTC);
            return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ULL + static_cast<uint64_t>(ts.tv_nsec);
        }

        [[nodiscard]] uint32_t next_pow2(uint32_t value) noexcept {
            uint32_t out = 1;
            while (out < value) { out <<= 1; }
            return out;
        }

        [[nodiscard]] uint32_t shards_for_threads(uint32_t writers, uint32_t cap) noexcept {
            if (writers <= 1) { return 1; }
            const uint32_t raw = (writers * (writers - 1u) * 9u + 3u) / 4u;
            return std::min(next_pow2(std::max(raw, 2u)), cap);
        }

        void ensure_dir(const fs::path& path) { if (!path.empty()) { fs::create_directories(path); } }

        [[nodiscard]] uint64_t load_or_create_node_id(const fs::path& path) {
            if (path.empty()) { return 0; }
            ensure_dir(path.parent_path());

            {
                std::ifstream in(path, std::ios::binary);
                uint64_t id = 0;
                if (in.read(reinterpret_cast<char*>(&id), sizeof(id)) && id != 0) { return id; }
            }

            std::random_device rd;
            std::mt19937_64 rng(rd());
            uint64_t id = rng();
            if (id == 0) { id = 1; }

            std::ofstream out(path, std::ios::binary | std::ios::trunc);
            if (!out.write(reinterpret_cast<const char*>(&id), sizeof(id))) {
                throw std::runtime_error("AkkEngine: failed to persist node id: " + path.string());
            }
            return id;
        }

        [[nodiscard]] int compare_key(std::span<const uint8_t> a, std::span<const uint8_t> b) {
            const int cmp = std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end())
                                ? -1
                                : std::lexicographical_compare(b.begin(), b.end(), a.begin(), a.end())
                                ? 1
                                : 0;
            return cmp;
        }

        [[nodiscard]] std::span<const uint8_t> copy_span_to_arena(std::span<const uint8_t> in, core::BufferArena& arena) {
            if (in.empty()) { return {}; }
            std::byte* raw = arena.allocate(in.size(), alignof(uint8_t));
            auto* bytes = reinterpret_cast<uint8_t*>(raw);
            std::memcpy(bytes, in.data(), in.size());
            return {bytes, in.size()};
        }
    } // namespace

    [[nodiscard]] std::optional<std::span<const uint8_t>> resolve_scan_value(
        uint8_t flags,
        std::span<const uint8_t> value,
        blob::BlobManager* blob_manager,
        core::BufferArena& arena
    ) {
        if ((flags & core::MemHdr16::FLAG_BLOB) == 0) { return value; }
        if (!blob_manager || value.size() < blob::BLOB_REF_SIZE) { return std::nullopt; }
        const blob::BlobRef ref = blob::decode_blob_ref(value.data());
        auto out = blob_manager->read(ref.blob_id, ref.content_crc32c);
        if (out.empty() && ref.total_size != 0) { return std::nullopt; }
        return copy_span_to_arena(std::span<const uint8_t>{out.data(), out.size()}, arena);
    }

    [[nodiscard]] core::ArenaGenerator<AkkEngine::ScanRecordView> scan_generator(
        core::BufferArena& arena,
        memtable::MemTable::RangeIterator memtable_iter,
        sst::SSTManager::Iterator sst_iter,
        blob::BlobManager* blob_manager
    ) {
        auto memtable_cur = memtable_iter.has_next() ? memtable_iter.next() : std::optional<core::RecordView>{};
        auto sst_cur = sst_iter.has_next() ? sst_iter.next() : std::optional<sst::SSTRecord>{};

        while (memtable_cur || sst_cur) {
            const bool has_mt = memtable_cur.has_value();
            const bool has_sst = sst_cur.has_value();
            int cmp = 0;
            if (has_mt && has_sst) { cmp = compare_key(memtable_cur->key(), sst_cur->key); }
            else { cmp = has_mt ? -1 : 1; }

            if (cmp <= 0) {
                const auto record = *memtable_cur;
                const bool tombstone = record.is_tombstone();
                if (!tombstone) {
                    auto value = resolve_scan_value(record.flags(), record.value(), blob_manager, arena);
                    if (value) { co_yield AkkEngine::ScanRecordView{record.key(), *value}; }
                }
                memtable_cur = memtable_iter.has_next() ? memtable_iter.next() : std::optional<core::RecordView>{};
                if (cmp == 0) { sst_cur = sst_iter.has_next() ? sst_iter.next() : std::optional<sst::SSTRecord>{}; }
            }
            else {
                const auto record = std::move(*sst_cur);
                const bool tombstone = record.is_tombstone();
                if (!tombstone) {
                    auto value = resolve_scan_value(record.flags, record.value, blob_manager, arena);
                    if (value) { co_yield AkkEngine::ScanRecordView{record.key, *value}; }
                }
                sst_cur = sst_iter.has_next() ? sst_iter.next() : std::optional<sst::SSTRecord>{};
            }
        }
    }

    class AkkEngine::Impl {
        public:
            AkkEngineOptions opts;
            std::atomic<bool> closed{false};
            uint64_t node_id = 0;

            std::unique_ptr<manifest::Manifest> manifest;
            std::unique_ptr<sst::SSTManager> sst_manager;
            std::unique_ptr<memtable::MemTable> memtable;
            std::unique_ptr<wal::WalWriter> wal_writer;
            std::unique_ptr<blob::BlobManager> blob_manager;
            std::unique_ptr<vlog::VersionLog> version_log;
            std::unique_ptr<cluster::ClusterRuntime> cluster_runtime;
            std::unique_ptr<server::AkkApiServer> api_server;

            mutable std::mutex write_mu;
            std::atomic<uint64_t> puts_total{0};
            std::atomic<uint64_t> removes_total{0};
            std::atomic<uint64_t> gets_total{0};
            std::atomic<uint64_t> gets_memtable_hit{0};
            std::atomic<uint64_t> gets_sst_hit{0};
            std::atomic<uint64_t> gets_miss{0};
            std::atomic<uint64_t> exists_total{0};
            std::atomic<uint64_t> scans_total{0};
            std::atomic<uint64_t> blob_puts_total{0};

            [[nodiscard]] uint64_t snapshot_seq() const noexcept { return memtable ? memtable->last_seq() : 0; }

            [[nodiscard]] bool persistent() const noexcept {
                return opts.components.wal_enabled || opts.components.sst_enabled || opts.components.manifest_enabled;
            }

            [[nodiscard]] std::vector<uint8_t> maybe_externalize(uint64_t seq, std::span<const uint8_t> value, uint8_t& flags) {
                if (!blob_manager || value.size() < blob_manager->threshold()) { return {value.begin(), value.end()}; }

                blob_manager->write(seq, value);
                std::vector<uint8_t> ref(blob::BLOB_REF_SIZE);
                blob::encode_blob_ref(ref.data(), blob::BlobRef{seq, static_cast<uint64_t>(value.size()), blob::crc32c(value)});
                flags |= core::MemHdr16::FLAG_BLOB;
                if (cluster_runtime) { cluster_runtime->ship_blob(seq, seq, value); }
                return ref;
            }

            [[nodiscard]] std::optional<std::vector<uint8_t>> resolve_value(uint8_t flags, std::span<const uint8_t> value) const {
                if ((flags & core::MemHdr16::FLAG_BLOB) == 0) { return std::vector<uint8_t>{value.begin(), value.end()}; }
                if (!blob_manager || value.size() < blob::BLOB_REF_SIZE) { return std::nullopt; }
                const blob::BlobRef ref = blob::decode_blob_ref(value.data());
                auto out = blob_manager->read(ref.blob_id, ref.content_crc32c);
                if (out.empty() && ref.total_size != 0) { return std::nullopt; }
                return out;
            }

            void append_all(
                uint64_t seq,
                std::span<const uint8_t> key,
                std::span<const uint8_t> stored_value,
                uint8_t flags,
                uint64_t source_node_id,
                uint64_t precomputed_fp64 = 0,
                uint64_t precomputed_mini_key = 0
            ) {
                const uint64_t fp64 = precomputed_fp64 != 0 ? precomputed_fp64 : core::compute_key_fp64(key);
                const uint64_t mini = precomputed_mini_key != 0 ? precomputed_mini_key : core::build_mini_key(key);
                if (wal_writer) { wal_writer->append(key, stored_value, seq, flags, fp64); }
                if (version_log) { version_log->append(key, seq, source_node_id, now_ns(), flags, stored_value); }

                if ((flags & core::MemHdr16::FLAG_TOMBSTONE) != 0) { memtable->remove(key, seq, fp64, mini); }
                else { memtable->put(key, stored_value, seq, flags, fp64, mini); }
            }

            void apply_replica_record(
                uint64_t seq,
                cluster::ReplOpType op,
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint8_t record_flags,
                uint64_t source_node_id
            ) {
                std::lock_guard lock(write_mu);
                uint8_t flags = record_flags;
                if (op == cluster::ReplOpType::Remove) { flags |= core::MemHdr16::FLAG_TOMBSTONE; }
                append_all(seq, key, value, flags, source_node_id);
                memtable->advance_seq(seq);
            }
    };

    AkkEngine::AkkEngine() = default;
    AkkEngine::~AkkEngine() { close(); }

    std::unique_ptr<AkkEngine> AkkEngine::open(AkkEngineOptions options) {
        auto fill_path = [&](fs::path& target, const char* fallback) {
            if (target.empty() && !options.paths.data_dir.empty()) { target = options.paths.data_dir / fallback; }
        };

        fill_path(options.paths.wal_dir, "wal");
        fill_path(options.paths.blob_dir, "blobs");
        fill_path(options.paths.sst_dir, "sstable");
        fill_path(options.paths.manifest_path, "manifest.akmf");
        fill_path(options.paths.version_log_path, "history.akvlog");
        fill_path(options.paths.cluster_config_path, "cluster.akcc");
        fill_path(options.paths.node_id_path, "node.id");

        if (options.wal.wal_dir.empty()) { options.wal.wal_dir = options.paths.wal_dir; }
        if (options.blob.blob_dir.empty()) { options.blob.blob_dir = options.paths.blob_dir; }
        if (options.sst.sst_dir.empty()) { options.sst.sst_dir = options.paths.sst_dir; }
        if (options.vlog.log_path.empty()) { options.vlog.log_path = options.paths.version_log_path; }

        if (options.runtime.writer_threads > 0) {
            if (options.memtable.expected_concurrent_writers == 0) { options.memtable.expected_concurrent_writers = options.runtime.writer_threads; }
            if (options.memtable.shard_count == 0) { options.memtable.shard_count = shards_for_threads(options.runtime.writer_threads, 256); }
            if (options.wal.shard_count == 0) { options.wal.shard_count = static_cast<uint16_t>(shards_for_threads(options.runtime.writer_threads, 64)); }
        }

        if (!options.paths.data_dir.empty()) { ensure_dir(options.paths.data_dir); }
        if (options.components.wal_enabled) { ensure_dir(options.wal.wal_dir); }
        if (options.components.blob_enabled) { ensure_dir(options.blob.blob_dir); }
        if (options.components.sst_enabled) { ensure_dir(options.sst.sst_dir); }
        if (options.components.manifest_enabled) { ensure_dir(options.paths.manifest_path.parent_path()); }
        if (options.components.version_log_enabled && options.vlog.log_path.empty()) {
            throw std::invalid_argument("AkkEngine: version log path is required when components.version_log_enabled is true");
        }
        if (options.components.version_log_enabled) { ensure_dir(options.vlog.log_path.parent_path()); }
        if (options.components.api_enabled && options.api.bind_host.empty()) {
            throw std::invalid_argument("AkkEngine: api.bind_host is required when components.api_enabled is true");
        }

        auto engine = std::unique_ptr < AkkEngine >
        {
            new AkkEngine()
        };
        engine->impl_ = std::make_unique<Impl>();
        Impl& impl = *engine->impl_;
        impl.opts = std::move(options);
        impl.node_id = load_or_create_node_id(impl.opts.paths.node_id_path);

        if (impl.opts.components.manifest_enabled && !impl.opts.paths.manifest_path.empty()) {
            impl.manifest = manifest::Manifest::create(impl.opts.paths.manifest_path, impl.opts.manifest.fast_mode);
            impl.manifest->start();
        }

        if (impl.opts.components.sst_enabled && !impl.opts.sst.sst_dir.empty()) {
            impl.sst_manager = sst::SSTManager::create(impl.opts.sst, impl.manifest.get());
            if (impl.opts.runtime.recover_sst) { impl.sst_manager->recover(); }
        }

        impl.opts.memtable.on_flush = [&impl](std::span<const core::RecordView> records) {
            if (!impl.sst_manager || records.empty()) { return; }
            const uint64_t checkpoint_seq = impl.sst_manager->flush(records);
            if (impl.wal_writer && impl.opts.runtime.prune_wal_on_flush) { impl.wal_writer->prune_until(checkpoint_seq); }
            if (impl.manifest) { impl.manifest->checkpoint(std::optional<std::string>{"flush"}, std::nullopt, checkpoint_seq); }
        };
        impl.memtable = memtable::MemTable::create(impl.opts.memtable);

        if (impl.opts.components.wal_enabled && impl.opts.runtime.recover_wal) {
            const auto recovery = wal::WalRecovery::recover_into(wal::WalRecoveryOptions{.wal_dir = impl.opts.wal.wal_dir}, *impl.memtable);
            (void)recovery;
        }

        if (impl.opts.components.wal_enabled) { impl.wal_writer = wal::WalWriter::create(impl.opts.wal); }

        if (impl.opts.components.blob_enabled) {
            impl.blob_manager = blob::BlobManager::create(impl.opts.blob);
            impl.blob_manager->start();
        }

        if (impl.opts.components.version_log_enabled) { impl.version_log = vlog::VersionLog::create(impl.opts.vlog); }

        if (impl.opts.components.cluster_enabled) {
            cluster::ClusterConfig cfg = impl.opts.cluster.config.has_value()
                                             ? *impl.opts.cluster.config
                                             : cluster::ClusterConfig::load(impl.opts.paths.cluster_config_path);
            cluster::ClusterEngineCallbacks callbacks;
            callbacks.get_current_seq = [&impl] { return impl.snapshot_seq(); };
            callbacks.get_last_seq = [&impl] { return impl.snapshot_seq(); };
            callbacks.apply = [&impl](
                uint64_t seq,
                cluster::ReplOpType op,
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint8_t record_flags,
                uint64_t source_node_id
            ) {
                    impl.apply_replica_record(seq, op, key, value, record_flags, source_node_id);
                };
            callbacks.apply_blob = [&impl](uint64_t /*seq*/, uint64_t blob_id, std::span<const uint8_t> content) {
                if (impl.blob_manager) { impl.blob_manager->write(blob_id, content); }
            };

            impl.cluster_runtime = cluster::ClusterRuntime::create(
                impl.opts.paths.data_dir,
                std::move(cfg),
                impl.node_id,
                std::move(callbacks),
                impl.opts.cluster.runtime
            );
            impl.cluster_runtime->start();
        }

        if (impl.opts.components.api_enabled) {
            impl.api_server = server::AkkApiServer::create(*engine, impl.opts.api);
            impl.api_server->start();
        }

        return engine;
    }

    void AkkEngine::put(std::span<const uint8_t> key, std::span<const uint8_t> value) {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        std::lock_guard lock(impl_->write_mu);

        const uint64_t seq = impl_->memtable->reserve_seq(1);
        uint8_t flags = core::MemHdr16::FLAG_NORMAL;
        std::vector<uint8_t> stored = impl_->maybe_externalize(seq, value, flags);
        impl_->puts_total.fetch_add(1, std::memory_order_relaxed);
        if ((flags & core::MemHdr16::FLAG_BLOB) != 0) { impl_->blob_puts_total.fetch_add(1, std::memory_order_relaxed); }
        impl_->append_all(seq, key, stored, flags, impl_->node_id);
        if (impl_->cluster_runtime) { impl_->cluster_runtime->ship_entry(seq, cluster::ReplOpType::Put, key, stored, flags, impl_->node_id); }
    }

    void AkkEngine::put_hinted(std::span<const uint8_t> key, std::span<const uint8_t> value, uint64_t fp64, uint64_t mini_key) {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        std::lock_guard lock(impl_->write_mu);

        const uint64_t seq = impl_->memtable->reserve_seq(1);
        uint8_t flags = core::MemHdr16::FLAG_NORMAL;
        std::vector<uint8_t> stored = impl_->maybe_externalize(seq, value, flags);
        impl_->puts_total.fetch_add(1, std::memory_order_relaxed);
        if ((flags & core::MemHdr16::FLAG_BLOB) != 0) { impl_->blob_puts_total.fetch_add(1, std::memory_order_relaxed); }
        impl_->append_all(seq, key, stored, flags, impl_->node_id, fp64, mini_key);
        if (impl_->cluster_runtime) { impl_->cluster_runtime->ship_entry(seq, cluster::ReplOpType::Put, key, stored, flags, impl_->node_id); }
    }

    void AkkEngine::remove(std::span<const uint8_t> key) {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        std::lock_guard lock(impl_->write_mu);

        const uint64_t seq = impl_->memtable->reserve_seq(1);
        constexpr uint8_t flags = core::MemHdr16::FLAG_TOMBSTONE;
        impl_->removes_total.fetch_add(1, std::memory_order_relaxed);
        impl_->append_all(seq, key, {}, flags, impl_->node_id);
        if (impl_->cluster_runtime) { impl_->cluster_runtime->ship_entry(seq, cluster::ReplOpType::Remove, key, {}, flags, impl_->node_id); }
    }

    void AkkEngine::remove_hinted(std::span<const uint8_t> key, uint64_t fp64, uint64_t mini_key) {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        std::lock_guard lock(impl_->write_mu);

        const uint64_t seq = impl_->memtable->reserve_seq(1);
        constexpr uint8_t flags = core::MemHdr16::FLAG_TOMBSTONE;
        impl_->removes_total.fetch_add(1, std::memory_order_relaxed);
        impl_->append_all(seq, key, {}, flags, impl_->node_id, fp64, mini_key);
        if (impl_->cluster_runtime) { impl_->cluster_runtime->ship_entry(seq, cluster::ReplOpType::Remove, key, {}, flags, impl_->node_id); }
    }

    std::optional<std::vector<uint8_t>> AkkEngine::get(std::span<const uint8_t> key) const {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        impl_->gets_total.fetch_add(1, std::memory_order_relaxed);
        const uint64_t seq = impl_->snapshot_seq();

        core::RecordView view;
        if (impl_->memtable->get(key, seq, &view)) {
            if (view.is_tombstone()) {
                impl_->gets_miss.fetch_add(1, std::memory_order_relaxed);
                return std::nullopt;
            }
            auto value = impl_->resolve_value(view.flags(), view.value());
            if (value) { impl_->gets_memtable_hit.fetch_add(1, std::memory_order_relaxed); }
            else { impl_->gets_miss.fetch_add(1, std::memory_order_relaxed); }
            return value;
        }

        if (impl_->sst_manager) {
            auto record = impl_->sst_manager->get(key);
            if (record) {
                if (record->is_tombstone()) {
                    impl_->gets_miss.fetch_add(1, std::memory_order_relaxed);
                    return std::nullopt;
                }
                if (impl_->opts.runtime.sst_promote_reads && impl_->memtable) {
                    impl_->memtable->put(record->key, record->value, record->seq, record->flags, record->key_fp64, record->mini_key);
                }
                auto value = impl_->resolve_value(record->flags, record->value);
                if (value) { impl_->gets_sst_hit.fetch_add(1, std::memory_order_relaxed); }
                else { impl_->gets_miss.fetch_add(1, std::memory_order_relaxed); }
                return value;
            }
        }
        impl_->gets_miss.fetch_add(1, std::memory_order_relaxed);
        return std::nullopt;
    }

    bool AkkEngine::exists(std::span<const uint8_t> key) const {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        impl_->exists_total.fetch_add(1, std::memory_order_relaxed);
        const uint64_t seq = impl_->snapshot_seq();
        if (const auto mt = impl_->memtable->contains(key, seq); mt.has_value()) { return *mt; }
        if (impl_->sst_manager) { if (const auto sst = impl_->sst_manager->contains(key); sst.has_value()) { return *sst; } }
        return false;
    }

    bool AkkEngine::get_into(std::span<const uint8_t> key, std::vector<uint8_t>& out) const {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        if (impl_->blob_manager) {
            auto value = get(key);
            if (!value) { return false; }
            out = std::move(*value);
            return true;
        }

        impl_->gets_total.fetch_add(1, std::memory_order_relaxed);
        const uint64_t seq = impl_->snapshot_seq();
        if (const auto mt = impl_->memtable->get_into(key, seq, out); mt.has_value()) {
            if (*mt) { impl_->gets_memtable_hit.fetch_add(1, std::memory_order_relaxed); }
            else { impl_->gets_miss.fetch_add(1, std::memory_order_relaxed); }
            return *mt;
        }
        if (impl_->sst_manager) {
            if (const auto sst = impl_->sst_manager->get_into(key, out); sst.has_value()) {
                if (*sst) {
                    if (impl_->opts.runtime.sst_promote_reads) {
                        if (auto record = impl_->sst_manager->get(key); record && !record->is_tombstone()) {
                            impl_->memtable->put(record->key, record->value, record->seq, record->flags, record->key_fp64, record->mini_key);
                        }
                    }
                    impl_->gets_sst_hit.fetch_add(1, std::memory_order_relaxed);
                }
                else { impl_->gets_miss.fetch_add(1, std::memory_order_relaxed); }
                return *sst;
            }
        }
        impl_->gets_miss.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    bool AkkEngine::get_into_arena(std::span<const uint8_t> key, core::BufferArena& arena, std::span<const uint8_t>& out) const {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        out = {};

        if (impl_->blob_manager) {
            auto value = get(key);
            if (!value) { return false; }
            out = copy_span_to_arena(*value, arena);
            return true;
        }

        impl_->gets_total.fetch_add(1, std::memory_order_relaxed);
        const uint64_t seq = impl_->snapshot_seq();
        core::RecordView view;
        if (impl_->memtable->get(key, seq, &view)) {
            if (view.is_tombstone()) {
                impl_->gets_miss.fetch_add(1, std::memory_order_relaxed);
                return false;
            }
            out = copy_span_to_arena(view.value(), arena);
            impl_->gets_memtable_hit.fetch_add(1, std::memory_order_relaxed);
            return true;
        }

        if (impl_->sst_manager) {
            auto record = impl_->sst_manager->get(key);
            if (record) {
                if (record->is_tombstone()) {
                    impl_->gets_miss.fetch_add(1, std::memory_order_relaxed);
                    return false;
                }
                if (impl_->opts.runtime.sst_promote_reads && impl_->memtable) {
                    impl_->memtable->put(record->key, record->value, record->seq, record->flags, record->key_fp64, record->mini_key);
                }
                out = copy_span_to_arena(record->value, arena);
                impl_->gets_sst_hit.fetch_add(1, std::memory_order_relaxed);
                return true;
            }
        }
        impl_->gets_miss.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    size_t AkkEngine::count(std::span<const uint8_t> start_key, std::span<const uint8_t> end_key) const {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }

        memtable::MemTable::KeyRange range;
        range.start.assign(start_key.begin(), start_key.end());
        range.end.assign(end_key.begin(), end_key.end());
        auto mt = impl_->memtable->iterator(range, impl_->snapshot_seq());
        sst::SSTManager::Iterator sst_it;
        if (impl_->sst_manager) { sst_it = impl_->sst_manager->scan_iter(start_key, end_key); }

        auto mt_cur = mt.has_next() ? mt.next() : std::optional<core::RecordView>{};
        auto sst_cur = sst_it.has_next() ? sst_it.next() : std::optional<sst::SSTRecord>{};
        size_t n = 0;

        while (mt_cur || sst_cur) {
            const bool has_mt = mt_cur.has_value();
            const bool has_sst = sst_cur.has_value();
            int cmp = 0;
            if (has_mt && has_sst) { cmp = compare_key(mt_cur->key(), sst_cur->key); }
            else { cmp = has_mt ? -1 : 1; }

            bool tombstone = false;
            if (cmp <= 0) {
                tombstone = mt_cur->is_tombstone();
                mt_cur = mt.has_next() ? mt.next() : std::optional<core::RecordView>{};
                if (cmp == 0) { sst_cur = sst_it.has_next() ? sst_it.next() : std::optional<sst::SSTRecord>{}; }
            }
            else {
                tombstone = sst_cur->is_tombstone();
                sst_cur = sst_it.has_next() ? sst_it.next() : std::optional<sst::SSTRecord>{};
            }

            if (!tombstone) { ++n; }
        }

        return n;
    }

    core::ArenaGenerator<AkkEngine::ScanRecordView> AkkEngine::scan(
        core::BufferArena& arena,
        std::span<const uint8_t> start_key,
        std::span<const uint8_t> end_key
    ) const {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        impl_->scans_total.fetch_add(1, std::memory_order_relaxed);

        memtable::MemTable::KeyRange range;
        range.start.assign(start_key.begin(), start_key.end());
        range.end.assign(end_key.begin(), end_key.end());
        auto mt = impl_->memtable->iterator(range, impl_->snapshot_seq());
        sst::SSTManager::Iterator st;
        if (impl_->sst_manager) { st = impl_->sst_manager->scan_iter(start_key, end_key); }
        return core::ArenaGenerator<ScanRecordView>::with_arena(
            arena,
            [&arena, mt = std::move(mt), st = std::move(st), blob_manager = impl_->blob_manager.get()]() mutable {
                return scan_generator(arena, std::move(mt), std::move(st), blob_manager);
            }
        );
    }

    std::optional<std::vector<uint8_t>> AkkEngine::get_at(std::span<const uint8_t> key, uint64_t at_seq) const {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        if (!impl_->version_log) { return std::nullopt; }
        auto entry = impl_->version_log->get_at(key, at_seq);
        if (!entry || (entry->flags & core::MemHdr16::FLAG_TOMBSTONE) != 0) { return std::nullopt; }
        return impl_->resolve_value(entry->flags, entry->value);
    }

    std::vector<VersionEntry> AkkEngine::history(std::span<const uint8_t> key) const {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        return impl_->version_log ? impl_->version_log->history(key) : std::vector<VersionEntry>{};
    }

    void AkkEngine::rollback_to(uint64_t target_seq) {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        if (!impl_->version_log) { throw std::runtime_error("AkkEngine: version log is disabled"); }
        for (const auto& [key, prev] : impl_->version_log->collect_rollback_targets(target_seq)) {
            if (!prev || (prev->flags & core::MemHdr16::FLAG_TOMBSTONE) != 0) { remove(key); }
            else {
                auto value = impl_->resolve_value(prev->flags, prev->value);
                if (value) { put(key, *value); }
                else { remove(key); }
            }
        }
    }

    void AkkEngine::rollback_key(std::span<const uint8_t> key, uint64_t target_seq) {
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        if (!impl_->version_log) { throw std::runtime_error("AkkEngine: version log is disabled"); }
        const auto prev = impl_->version_log->get_at(key, target_seq);
        if (!prev || (prev->flags & core::MemHdr16::FLAG_TOMBSTONE) != 0) { remove(key); }
        else {
            auto value = impl_->resolve_value(prev->flags, prev->value);
            if (value) { put(key, *value); }
            else { remove(key); }
        }
    }

    EngineStats AkkEngine::stats() const noexcept {
        EngineStats out;
        if (!impl_ || impl_->closed.load(std::memory_order_acquire)) { return out; }

        out.current_seq = impl_->snapshot_seq();
        out.node_id = impl_->node_id;

        out.puts_total = impl_->puts_total.load(std::memory_order_relaxed);
        out.removes_total = impl_->removes_total.load(std::memory_order_relaxed);
        out.gets_total = impl_->gets_total.load(std::memory_order_relaxed);
        out.gets_memtable_hit = impl_->gets_memtable_hit.load(std::memory_order_relaxed);
        out.gets_sst_hit = impl_->gets_sst_hit.load(std::memory_order_relaxed);
        out.gets_miss = impl_->gets_miss.load(std::memory_order_relaxed);
        out.exists_total = impl_->exists_total.load(std::memory_order_relaxed);
        out.scans_total = impl_->scans_total.load(std::memory_order_relaxed);
        out.blob_puts_total = impl_->blob_puts_total.load(std::memory_order_relaxed);

        if (impl_->memtable) {
            const auto snap = impl_->memtable->snapshot();
            out.memtable.shard_count = snap.shard_count;
            out.memtable.threshold_bytes_per_shard = snap.threshold_bytes_per_shard;
            out.memtable.approx_bytes = snap.approx_bytes;
            out.memtable.puts_applied = snap.puts_applied;
            out.memtable.removes_applied = snap.removes_applied;
            out.memtable.flushes_completed = snap.flushes_completed;
        }

        out.wal.enabled = impl_->opts.components.wal_enabled;
        if (impl_->wal_writer) {
            const auto snap = impl_->wal_writer->snapshot();
            out.wal.shard_count = snap.shard_count;
            out.wal.entries_written = snap.entries_written;
            out.wal.bytes_written = snap.bytes_written;
            out.wal.batches_flushed = snap.batches_flushed;
            out.wal.syncs_executed = snap.syncs_executed;
            out.wal.segment_rotations = snap.segment_rotations;
        }

        out.blob.enabled = impl_->opts.components.blob_enabled && impl_->blob_manager != nullptr;
        out.blob.threshold_bytes = impl_->opts.blob.threshold_bytes;
        if (impl_->blob_manager) {
            const auto snap = impl_->blob_manager->snapshot();
            out.blob.blobs_written = snap.blobs_written;
            out.blob.bytes_uncompressed = snap.bytes_uncompressed;
            out.blob.bytes_on_disk = snap.bytes_on_disk;
            out.blob.blobs_deleted = snap.blobs_deleted;
            out.blob.gc_cycles = snap.gc_cycles;
        }

        out.sst.enabled = impl_->sst_manager != nullptr;
        if (impl_->sst_manager) {
            const auto levels = impl_->sst_manager->level_stats();
            out.sst.levels.reserve(levels.size());
            for (const auto& level : levels) {
                out.sst.levels.push_back(LevelStats{level.level, level.file_count, level.bytes, level.budget_bytes});
                out.sst.file_count += level.file_count;
                out.sst.bytes += level.bytes;
                if (level.level == 0) { out.sst.l0_file_count = level.file_count; }
            }
            out.sst.compaction_pending = impl_->sst_manager->compaction_pending();
            const auto snap = impl_->sst_manager->compaction_snapshot();
            out.sst.compactions_completed = snap.compactions_completed;
            out.sst.files_compacted = snap.files_compacted;
            out.sst.bytes_compacted_in = snap.bytes_compacted_in;
            out.sst.bytes_compacted_out = snap.bytes_compacted_out;
        }

        out.vlog.enabled = impl_->version_log != nullptr;
        return out;
    }

    void AkkEngine::force_sync() {
        if (impl_&& impl_
        ->
        wal_writer
        )
        {
            impl_->wal_writer->force_sync();
        }
    }

    void AkkEngine::force_flush() {
        if (impl_&& impl_
        ->
        memtable
        )
        {
            impl_->memtable->force_flush();
        }
    }

    void AkkEngine::close() {
        if (!impl_) { return; }
        bool expected = false;
        if (!impl_->closed.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) { return; }

        if (impl_->api_server) {
            impl_->api_server->close();
            impl_->api_server.reset();
        }
        if (impl_->cluster_runtime) {
            impl_->cluster_runtime->close();
            impl_->cluster_runtime.reset();
        }
        if (impl_->memtable && impl_->opts.runtime.force_flush_on_close) { impl_->memtable->force_flush(); }
        if (impl_->sst_manager) {
            impl_->sst_manager->shutdown();
            impl_->sst_manager.reset();
        }
        if (impl_->wal_writer) {
            if (impl_->opts.runtime.force_sync_on_close) { impl_->wal_writer->force_sync(); }
            impl_->wal_writer->close();
            impl_->wal_writer.reset();
        }
        if (impl_->manifest) {
            impl_->manifest->close();
            impl_->manifest.reset();
        }
        if (impl_->blob_manager) {
            impl_->blob_manager->close();
            impl_->blob_manager.reset();
        }
        if (impl_->version_log) {
            impl_->version_log->close();
            impl_->version_log.reset();
        }
        impl_->memtable.reset();
    }
} // namespace akkaradb::engine

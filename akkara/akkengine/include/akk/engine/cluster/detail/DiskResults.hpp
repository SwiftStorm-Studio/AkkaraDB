/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
#pragma once

#include "akk/engine/AkkEngine.hpp"
#include <algorithm>
#include <functional>
#include <mutex>
#include <thread>
#include <condition_variable>

namespace akkaradb::engine::cluster::detail {
    // A private standalone WAL/SST engine keeps retained results out of the
    // consensus heap. Generation replacement is published only after staging
    // has reached stable storage; old generations remain readable by snapshots.
    class DiskResults : public std::enable_shared_from_this<DiskResults> {
    public:
        struct Record { uint64_t expires = 0, sequence = 0; std::vector<uint8_t> value; };
        using Visitor = std::function<bool(std::span<const uint8_t>, const Record&)>;
        struct Snapshot {
            std::shared_ptr<DiskResults> owner;
            std::shared_ptr<AkkEngine::Transaction> transaction;
            uint64_t generation = 0, sequence = 0, floor = 0, count = 0, bytes = 0;
            bool forEach(const Visitor& visitor) const {
                core::BufferArena arena;
                const auto start = prefix('R', generation), end = prefix('R', generation + 1);
                for (const auto& row : transaction->scan(arena, start, end)) {
                    if ((row.key.size() != 33 && row.key.size() != 41) || row.value.size() < 8) { throw std::runtime_error("Result store: malformed record"); }
                    Record record{integer(row.key.subspan(9, 8)), integer(row.value.first(8)),
                        {row.value.begin() + 8, row.value.end()}};
                    if (record.expires > floor && !visitor(row.key.subspan(17), record)) { return false; }
                    arena.reset();
                }
                return true;
            }
        };
        struct Import { uint64_t generation = 0, sequence = 0, floor = 0, count = 0, bytes = 0; };
    private:
        std::unique_ptr<AkkEngine> engine_;
        mutable std::mutex mutex_;
        const uint64_t limit_;
        bool closed_ = false;
        bool dirty_ = false;
        uint64_t generation_ = 1, sequence_ = 0, floor_ = 0, count_ = 0, bytes_ = 0, nextGeneration_ = 1;
        std::condition_variable gcCv_;
        uint64_t gcBefore_ = 0;
        std::jthread gcThread_;
        static std::vector<uint8_t> number(uint64_t value) {
            std::vector<uint8_t> bytes(8);
            for (size_t i = 0; i < 8; ++i) { bytes[i] = static_cast<uint8_t>(value >> (56 - i * 8)); }
            return bytes;
        }
        static uint64_t integer(std::span<const uint8_t> bytes) {
            if (bytes.size() != 8) { throw std::runtime_error("Result store: malformed integer"); }
            uint64_t value = 0; for (auto byte : bytes) { value = (value << 8) | byte; } return value;
        }
        static std::vector<uint8_t> prefix(uint8_t type, uint64_t generation) {
            auto key = number(generation); key.insert(key.begin(), type); return key;
        }
        static std::vector<uint8_t> indexKey(uint64_t generation, std::span<const uint8_t> id) {
            auto key = prefix('I', generation); key.insert(key.end(), id.begin(), id.end()); return key;
        }
        static std::vector<uint8_t> recordKey(uint64_t generation, uint64_t expires, std::span<const uint8_t> id) {
            auto key = prefix('R', generation); const auto expiry = number(expires);
            key.insert(key.end(), expiry.begin(), expiry.end()); key.insert(key.end(), id.begin(), id.end()); return key;
        }
        static uint64_t charge(size_t idBytes, size_t valueBytes) { return 42 + 2 * idBytes + valueBytes; }
        std::optional<Record> getLocked(uint64_t generation, std::span<const uint8_t> id) const {
            const auto index = engine_->get(indexKey(generation, id));
            if (!index) { return {}; }
            const auto expires = integer(*index);
            const auto value = engine_->get(recordKey(generation, expires, id));
            if (!value || value->size() < 8) { throw std::runtime_error("Result store: missing indexed record"); }
            return Record{expires, integer(std::span{*value}.first(8)), {value->begin() + 8, value->end()}};
        }
        void metadataLocked(uint64_t generation, uint64_t sequence, uint64_t floor, uint64_t count, uint64_t bytes) {
            std::vector<uint8_t> value;
            for (auto field : {sequence, floor, count, bytes}) { const auto encoded = number(field); value.insert(value.end(), encoded.begin(), encoded.end()); }
            engine_->put(prefix('M', generation), value);
        }
        void putLocked(uint64_t generation, std::span<const uint8_t> id, const Record& record) {
            if ((id.size() != 16 && id.size() != 24) || !record.expires || !record.sequence) {
                throw std::runtime_error("Result store: invalid result identity");
            }
            auto value = number(record.sequence); value.insert(value.end(), record.value.begin(), record.value.end());
            engine_->put(recordKey(generation, record.expires, id), value);
            engine_->put(indexKey(generation, id), number(record.expires));
        }
    public:
        DiskResults(const std::filesystem::path& directory, uint64_t limit) : limit_{limit} {
            std::filesystem::create_directories(directory);
            AkkEngineOptions options; options.paths.dataDir = directory;
            options.components.blobEnabled = options.components.versionLogEnabled = false;
            options.runtime.writeDurability = AkkEngineOptions::WriteDurabilityMode::WRITTEN;
            options.runtime.scanConsistency = AkkEngineOptions::ScanConsistencyMode::PINNED_SNAPSHOT;
            options.memtable.shardCount = 1;
            options.wal.execution = wal::WalExecutionMode::INLINE;
            options.wal.syncPolicy = wal::WalSyncPolicy::ON_SYNC_ACK;
            options.wal.shardCount = 1;
            options.memtable.thresholdBytesPerShard = 8ull * 1024 * 1024;
            options.sst.blockCacheBytes = 4ull * 1024 * 1024;
            options.sst.trackBlobReferences = false;
            options.transactions.maxOpen = 1024;
            options.transactions.maxLifetimeMs = 3'600'000;
            options.transactions.maxPinnedBytes = 64ull * 1024 * 1024;
            engine_ = AkkEngine::open(std::move(options));
            const std::array<uint8_t, 1> formatKey{'F'};
            const std::vector<uint8_t> format{'A','K','D','R','1'};
            if (const auto saved = engine_->get(formatKey)) {
                if (*saved != format) { throw std::runtime_error("Result store: incompatible storage format"); }
            } else { engine_->put(formatKey, format); }

            if (const auto generation = engine_->get(std::array<uint8_t, 1>{'G'})) { generation_ = integer(*generation); }
            if (const auto next = engine_->get(std::array<uint8_t, 1>{'N'})) { nextGeneration_ = integer(*next); }
            nextGeneration_ = std::max(nextGeneration_, generation_);
            if (!generation_ || generation_ == UINT64_MAX) { throw std::runtime_error("Result store: invalid generation"); }
            if (const auto metadata = engine_->get(prefix('M', generation_))) {
                if (metadata->size() != 32) { throw std::runtime_error("Result store: malformed metadata"); }
                sequence_ = integer(std::span{*metadata}.subspan(0, 8)); floor_ = integer(std::span{*metadata}.subspan(8, 8));
            }
            // A committed apply may have written its result before publishing
            // the counter checkpoint. Rebuild counters and repair its index.
            auto transaction = engine_->begin({TransactionIsolation::SNAPSHOT_ISOLATION});
            core::BufferArena arena;
            for (const auto& row : transaction.scan(arena, prefix('R', generation_), prefix('R', generation_ + 1))) {
                if ((row.key.size() != 33 && row.key.size() != 41) || row.value.size() < 8) { throw std::runtime_error("Result store: malformed recovered record"); }
                const auto expires = integer(row.key.subspan(9, 8)), sequence = integer(row.value.first(8));
                if (expires > floor_) {
                    bytes_ += charge(row.key.size() - 17, row.value.size() - 8); ++count_;
                    sequence_ = std::max(sequence_, sequence);
                    engine_->put(indexKey(generation_, row.key.subspan(17)), number(expires));
                }
                arena.reset();
            }
            if (bytes_ > limit_) { throw std::runtime_error("Result store: retained results exceed configured disk budget"); }
            metadataLocked(generation_, sequence_, floor_, count_, bytes_); engine_->forceSync();
            gcThread_ = std::jthread([this](std::stop_token stop) { garbageCollectionLoop(stop); });
        }
        ~DiskResults() { try { close(); } catch (...) {} }
        void close() {
            gcThread_.request_stop(); gcCv_.notify_all(); if (gcThread_.joinable()) { gcThread_.join(); }
            std::lock_guard lock{mutex_}; if (closed_) { return; } closed_ = true; engine_->close();
        }
        bool isClosed() const { std::lock_guard lock{mutex_}; return closed_; }
        std::optional<Record> get(std::span<const uint8_t> id) const {
            std::lock_guard lock{mutex_}; const auto record = getLocked(generation_, id);
            return record && record->expires > floor_ ? record : std::nullopt;
        }
        bool canReserve(uint64_t bytes, uint64_t now = 0) const {
            std::lock_guard lock{mutex_};
            if (bytes > limit_) { return false; }
            if (bytes_ <= limit_ - bytes) { return true; }
            uint64_t retained = bytes_; core::BufferArena arena;
            auto end = prefix('R', generation_); const auto expiry = number(now == UINT64_MAX ? now : now + 1);
            end.insert(end.end(), expiry.begin(), expiry.end());
            for (const auto& row : engine_->scanLocalStorage(arena, prefix('R', generation_), end)) {
                if ((row.key.size() != 33 && row.key.size() != 41) || row.value.size() < 8) { throw std::runtime_error("Result store: malformed expiry record"); }
                if (integer(row.key.subspan(9, 8)) > floor_) { retained -= charge(row.key.size() - 17, row.value.size() - 8); }
                arena.reset(); if (retained <= limit_ - bytes) { return true; }
            }
            return false;
        }
        uint64_t count() const { std::lock_guard lock{mutex_}; return count_; }
        uint64_t bytes() const { std::lock_guard lock{mutex_}; return bytes_; }
        uint64_t sequence() const { std::lock_guard lock{mutex_}; return sequence_; }
        uint64_t floor() const { std::lock_guard lock{mutex_}; return floor_; }
        uint64_t generation() const { std::lock_guard lock{mutex_}; return generation_; }
        void put(std::span<const uint8_t> id, const Record& record) {
            std::lock_guard lock{mutex_}; if (record.expires <= floor_) { return; }
            const auto old = getLocked(generation_, id);
            if (old && (old->expires != record.expires || old->sequence > record.sequence)) { throw std::runtime_error("Result store: conflicting result replacement"); }
            const auto before = old && old->expires > floor_ ? charge(id.size(), old->value.size()) : 0;
            const auto after = charge(id.size(), record.value.size());
            if (after > limit_ || bytes_ - before > limit_ - after) { throw std::length_error("Result store: retained-result disk budget exhausted"); }
            putLocked(generation_, id, record);
            bytes_ = bytes_ - before + after; if (!before) { ++count_; }
            sequence_ = std::max(sequence_, record.sequence); dirty_ = true;
        }
        void advanceFloor(uint64_t floor) {
            std::lock_guard lock{mutex_}; const auto oldFloor = floor_; floor_ = std::max(floor_, floor);
            if (floor_ != oldFloor) { dirty_ = true; }
            core::BufferArena arena;
            auto end = prefix('R', generation_); const auto upper = number(floor_ == UINT64_MAX ? floor_ : floor_ + 1);
            end.insert(end.end(), upper.begin(), upper.end());
            for (;;) {
                struct Expired { std::vector<uint8_t> key; uint64_t size, expires; };
                std::vector<Expired> expired;
                {
                    auto rows = engine_->scanLocalStorage(arena, prefix('R', generation_), end);
                    for (const auto& row : rows) {
                        if ((row.key.size() != 33 && row.key.size() != 41) || row.value.size() < 8) { throw std::runtime_error("Result store: invalid expiry index"); }
                        expired.push_back({{row.key.begin(), row.key.end()}, charge(row.key.size() - 17, row.value.size() - 8),
                            integer(row.key.subspan(9, 8))});
                        arena.reset(); if (expired.size() == 256) { break; }
                    }
                }
                for (const auto& record : expired) {
                    engine_->remove(record.key); engine_->remove(indexKey(generation_, std::span{record.key}.subspan(17))); dirty_ = true;
                    if (record.expires > oldFloor) { bytes_ -= record.size; --count_; }
                }
                if (expired.size() < 256) { break; }
            }
        }
        void sync() {
            std::lock_guard lock{mutex_};
            if (!dirty_) { return; }
            metadataLocked(generation_, sequence_, floor_, count_, bytes_); engine_->forceSync(); dirty_ = false;
        }
        Snapshot capture() {
            std::lock_guard lock{mutex_};
            return {shared_from_this(), std::make_shared<AkkEngine::Transaction>(engine_->begin({TransactionIsolation::SNAPSHOT_ISOLATION})),
                generation_, sequence_, floor_, count_, bytes_};
        }
        Import beginImport(uint64_t sequence, uint64_t floor) {
            std::lock_guard lock{mutex_};
            if (nextGeneration_ >= UINT64_MAX - 1) { throw std::runtime_error("Result store: generations exhausted"); }
            ++nextGeneration_; engine_->put(std::array<uint8_t, 1>{'N'}, number(nextGeneration_)); engine_->forceSync();
            return {nextGeneration_, sequence, floor, 0, 0};
        }
        void import(Import& target, std::span<const uint8_t> id, const Record& record) {
            std::lock_guard lock{mutex_};
            if (record.sequence > target.sequence || record.expires <= target.floor || getLocked(target.generation, id)) {
                throw std::runtime_error("Result store: invalid snapshot result");
            }
            const auto size = charge(id.size(), record.value.size());
            if (size > limit_ || target.bytes > limit_ - size) { throw std::length_error("Result store: snapshot exceeds retained-result disk budget"); }
            putLocked(target.generation, id, record); target.bytes += size; ++target.count;
        }
        void prepare(const Import& target) {
            std::lock_guard lock{mutex_}; metadataLocked(target.generation, target.sequence, target.floor, target.count, target.bytes); engine_->forceSync();
        }
        void activate(uint64_t generation) {
            std::lock_guard lock{mutex_}; const auto metadata = engine_->get(prefix('M', generation));
            if (!metadata || metadata->size() != 32) { throw std::runtime_error("Result store: missing prepared snapshot generation"); }
            engine_->put(std::array<uint8_t, 1>{'G'}, number(generation)); engine_->forceSync();
            dirty_ = false; generation_ = generation; sequence_ = integer(std::span{*metadata}.subspan(0, 8)); floor_ = integer(std::span{*metadata}.subspan(8, 8));
            count_ = integer(std::span{*metadata}.subspan(16, 8)); bytes_ = integer(std::span{*metadata}.subspan(24, 8));
        }
        // Only generations older than a finalized reference are reclaimable.
        // A later import may already be staging concurrently. Existing snapshot
        // transactions pin their sources while the worker removes old heads.
        void collectGarbage() {
            std::lock_guard lock{mutex_}; gcBefore_ = std::max(gcBefore_, generation_); gcCv_.notify_all();
        }
    private:
        void garbageCollectionLoop(std::stop_token stop) {
            try {
                uint64_t completed = 0;
                while (!stop.stop_requested()) {
                    uint64_t before = 0;
                    {
                        std::unique_lock lock{mutex_};
                        gcCv_.wait_for(lock, std::chrono::milliseconds{250}, [&] { return stop.stop_requested() || gcBefore_ > completed; });
                        if (stop.stop_requested()) { return; }
                        before = gcBefore_; if (before <= completed) { continue; }
                    }
                    core::BufferArena arena;
                    for (const auto type : {'I', 'M', 'R'}) {
                        std::vector<uint8_t> cursor{static_cast<uint8_t>(type)};
                        const std::vector<uint8_t> end{static_cast<uint8_t>(type + 1)};
                        while (!stop.stop_requested()) {
                            size_t scanned = 0;
                            {
                                std::lock_guard lock{mutex_};
                                std::vector<std::vector<uint8_t>> obsolete;
                                {
                                    auto rows = engine_->scanLocalStorage(arena, cursor, end);
                                    for (const auto& row : rows) {
                                        if (row.key.size() < 9) { throw std::runtime_error("Result store: invalid generation key"); }
                                        cursor.assign(row.key.begin(), row.key.end()); cursor.push_back(0);
                                        const auto generation = integer(row.key.subspan(1, 8));
                                        if (generation < before && generation != generation_) { obsolete.emplace_back(row.key.begin(), row.key.end()); }
                                        arena.reset(); if (++scanned == 256) { break; }
                                    }
                                }
                                for (const auto& key : obsolete) { engine_->remove(key); }
                            }
                            if (scanned < 256) { break; }
                            std::this_thread::yield();
                        }
                    }
                    completed = before;
                }
            } catch (...) {
                // Reclamation is optional. Retained results and the finalized
                // generation remain authoritative if cleanup cannot progress.
            }
        }
    public:
        static uint64_t recordCharge(size_t idBytes, size_t valueBytes) { return charge(idBytes, valueBytes); }
    };
}

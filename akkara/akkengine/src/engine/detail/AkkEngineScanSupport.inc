/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/AkkEngineScanSupport.inc
// Included inside detail::runtime by AkkEngineSupport.hpp.
    [[nodiscard]] inline int compareKey(std::span<const uint8_t> a, std::span<const uint8_t> b) {
        const int cmp = std::ranges::lexicographical_compare(a, b) ? -1 : std::ranges::lexicographical_compare(b, a) ? 1 : 0;
        return cmp;
    }

    [[nodiscard]] inline std::span<const uint8_t> copySpanToArena(std::span<const uint8_t> in, BufferArena& arena) {
        if (in.empty()) { return {}; }
        auto* raw = arena.allocate(in.size(), alignof(uint8_t));
        auto* bytes = reinterpret_cast<uint8_t*>(raw);
        std::memcpy(bytes, in.data(), in.size());
        return {bytes, in.size()};
    }

    [[nodiscard]] inline std::optional<uint64_t> decodeBlobIdIfReference(uint8_t flags, std::span<const uint8_t> value) noexcept {
        if ((flags & MemHdr16::FLAG_BLOB) == 0 || value.size() < blob::BLOB_REF_SIZE) { return std::nullopt; }
        return blob::decodeBlobRef(value.data()).blobId;
    }
    [[nodiscard]] inline std::optional<std::span<const uint8_t>> resolveScanValue(
        uint8_t flags,
        std::span<const uint8_t> value,
        blob::BlobManager* blobManager,
        BufferArena& arena
    ) {
        if ((flags & MemHdr16::FLAG_BLOB) == 0) { return value; }
        if (!blobManager || value.size() < blob::BLOB_REF_SIZE) { return std::nullopt; }
        const auto ref = blob::decodeBlobRef(value.data());
        auto out = blobManager->read(ref.blobId, ref.contentCrc32c);
        if (out.empty() && ref.totalSize != 0) { return std::nullopt; }
        return copySpanToArena(std::span<const uint8_t>{out.data(), out.size()}, arena);
    }

    struct StoredScanRecordView {
        std::span<const uint8_t> key;
        std::span<const uint8_t> value;
        uint8_t flags = 0;
    };

    [[nodiscard]] inline ArenaGenerator<StoredScanRecordView> storedScanGenerator(
        memtable::MemTable::RangeIterator memtableIter,
        sst::SSTManager::Iterator sstIter,
        std::shared_ptr<void> operationLifetime = {}
    ) {
        // Retains a public operation guard for the lifetime of a returned scan.
        (void)operationLifetime;
        auto memtableCur = memtableIter.hasNext() ? memtableIter.next() : std::optional<RecordView>{};
        auto sstCur = sstIter.hasNext() ? sstIter.next() : std::optional<sst::SSTRecord>{};

        while (memtableCur || sstCur) {
            const bool hasMt = memtableCur.has_value();
            const bool hasSst = sstCur.has_value();
            int cmp = 0;
            if (hasMt && hasSst) { cmp = compareKey(memtableCur->key(), sstCur->key); }
            else { cmp = hasMt ? -1 : 1; }

            if (cmp == 0) {
                // Frozen MemTables can overlap newer SSTs after asynchronous
                // flush publication. Resolve versions before hiding tombstones.
                if (memtableCur->seq() >= sstCur->seq) {
                    if (!memtableCur->isTombstone()) { co_yield StoredScanRecordView{memtableCur->key(), memtableCur->value(), memtableCur->flags()}; }
                }
                else if (!sstCur->isTombstone()) { co_yield StoredScanRecordView{sstCur->key, sstCur->value, sstCur->flags}; }
                memtableCur = memtableIter.hasNext() ? memtableIter.next() : std::optional<RecordView>{};
                sstCur = sstIter.hasNext() ? sstIter.next() : std::optional<sst::SSTRecord>{};
            }
            else if (cmp < 0) {
                const auto record = *memtableCur;
                if (!record.isTombstone()) { co_yield StoredScanRecordView{record.key(), record.value(), record.flags()}; }
                memtableCur = memtableIter.hasNext() ? memtableIter.next() : std::optional<RecordView>{};
            }
            else {
                const auto record = std::move(*sstCur);
                const bool tombstone = record.isTombstone();
                if (!tombstone) { co_yield StoredScanRecordView{record.key, record.value, record.flags}; }
                sstCur = sstIter.hasNext() ? sstIter.next() : std::optional<sst::SSTRecord>{};
            }
        }
    }

    [[nodiscard]] inline ArenaGenerator<AkkEngine::ScanRecordView> scanGenerator(
        BufferArena& arena,
        memtable::MemTable::RangeIterator memtableIter,
        sst::SSTManager::Iterator sstIter,
        blob::BlobManager* blobManager,
        blob::BlobManager::ReadPin blobReadPin,
        std::shared_ptr<void> operationLifetime = {}
    ) {
        // The parameter protects even an unstarted generator. Moving it into
        // a local releases the lease at completion or exception, before the
        // caller destroys an already exhausted generator.
        auto readPin = std::move(blobReadPin);
        (void)readPin;
        for (const auto& [key, value, flags] : storedScanGenerator(std::move(memtableIter), std::move(sstIter), std::move(operationLifetime))) {
            if (auto v = resolveScanValue(flags, value, blobManager, arena)) { co_yield AkkEngine::ScanRecordView{.key = key, .value = *v}; }
        }
    }

/*
 * AkkaraDB
 * Copyright (C) 2026 Swift Storm Studio
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. See https://mozilla.org/MPL/2.0/.
 */
#pragma once

#include <atomic>
#include <new>

#include "akk/core/record/OwnedRecord.hpp"
#include "akk/core/record/RecordView.hpp"
#include "akk/core/utils/ArenaGenerator.hpp"

namespace akkaradb::engine::memtable::detail {
    // Insert into a descending-sequence ring, retaining its highest sequences.
    // Equal-sequence reapplication replaces the existing record.
    template <typename Load, typename Store>
    bool insertSequenceOrdered(uint16_t capacity, uint16_t& head, uint16_t& count,
        const core::OwnedRecord* record, Load load, Store store) noexcept {
        const auto index = [&](uint16_t position) { return static_cast<uint16_t>((head + capacity - position) % capacity); };
        uint16_t position = 0;
        while (position < count && load(index(position))->seq() > record->seq()) { ++position; }
        if (position < count && load(index(position))->seq() == record->seq()) {
            store(index(position), record);
            return false;
        }
        if (position == capacity) { return false; }
        const bool grew = count < capacity;
        if (position == 0) {
            head = static_cast<uint16_t>((head + 1) % capacity);
            store(head, record);
            if (grew) { ++count; }
            return grew;
        }
        if (grew) { ++count; }
        for (uint16_t i = static_cast<uint16_t>(count - 1); i > position; --i) {
            store(index(i), load(index(static_cast<uint16_t>(i - 1))));
        }
        store(index(position), record);
        return grew;
    }

    // Immutable links let mutable readers retain any publication cut without
    // overwriting a committed version while newer writes are still unpublished.
    struct VersionHistory {
        struct Entry {
            const core::OwnedRecord* record;
            const Entry* previous;
            const core::OwnedRecord* maximum;
        };
        std::atomic<const Entry*> latest{nullptr};
        bool sequenceOrdered = false;

        void append(const core::OwnedRecord* record, core::BufferArena& arena,
            std::atomic<size_t>& bytes, const VersionHistory* previous = nullptr) {
            const auto* head = (previous ? previous : this)->latest.load(std::memory_order_acquire);
            const auto* maximum = head && head->maximum->seq() > record->seq() ? head->maximum : record;
            auto* entry = new(arena.allocate(sizeof(Entry), alignof(Entry))) Entry{record, head, maximum};
            bytes.fetch_add(sizeof(Entry), std::memory_order_relaxed);
            latest.store(entry, std::memory_order_release);
        }

        static core::RecordView view(const core::OwnedRecord& record) noexcept {
            return {record.key().data(), record.hdr.kLen, record.value().data(), record.hdr.vLen,
                record.hdr.seq, record.hdr.flags, record.keyFp64, record.miniKey};
        }

        bool get(uint64_t seq, core::RecordView* out) const noexcept {
            const auto* head = latest.load(std::memory_order_acquire);
            if (sequenceOrdered && head && head->maximum->seq() <= seq) {
                *out = view(*head->maximum);
                return true;
            }
            const core::OwnedRecord* best = nullptr;
            for (auto* entry = head; entry; entry = entry->previous) {
                if (entry->record->seq() <= seq && (!best || entry->record->seq() > best->seq())) {
                    best = entry->record;
                    if (!sequenceOrdered) { break; }
                }
                // A suffix maximum proves no older link can improve the result.
                // Monotonic writes therefore stop at the first visible version.
                if (sequenceOrdered && best && (!entry->previous || entry->previous->maximum->seq() <= best->seq())) { break; }
            }
            if (best) { *out = view(*best); }
            return best != nullptr;
        }

        core::ArenaGenerator<core::RecordView> records() const {
            for (auto* entry = latest.load(std::memory_order_acquire); entry; entry = entry->previous) {
                co_yield view(*entry->record);
            }
        }
    };
}

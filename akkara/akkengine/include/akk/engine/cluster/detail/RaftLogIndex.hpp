/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
#pragma once

#include <cstdint>
#include <cstddef>
#include <deque>
#include <stdexcept>

namespace akkaradb::engine::cluster::detail {
    // The durable prefix is immutable. Append extends its last extent; conflict
    // replacement and compaction discard only the affected suffix or prefix.
    class RaftLogIndex {
    public:
        struct Location { uint64_t index, term, segment, begin, end; };
        struct Extent { uint64_t segment, begin, end; };
        bool reconcile(uint64_t firstIndex, uint64_t afterIndex, uint64_t changedFrom) {
            bool removed = false;
            while (!locations_.empty() && locations_.front().index < firstIndex) {
                locations_.pop_front(); removed = true;
            }
            if (!locations_.empty() && locations_.front().index != firstIndex) {
                locations_.clear(); removed = true;
            }
            while (!locations_.empty() && (locations_.back().index >= afterIndex || locations_.back().index >= changedFrom)) {
                locations_.pop_back(); removed = true;
            }
            if (locations_.empty()) { extents_.clear(); }
            else {
                const auto& first = locations_.front();
                const auto& last = locations_.back();
                while (extents_.front().segment != first.segment || extents_.front().end <= first.begin) { extents_.pop_front(); }
                extents_.front().begin = first.begin;
                while (extents_.back().segment != last.segment || extents_.back().begin >= last.end) { extents_.pop_back(); }
                extents_.back().end = last.end;
            }
            return removed;
        }
        void append(Location location) {
            if ((!locations_.empty() && location.index != locations_.back().index + 1) ||
                location.segment == 0 || location.begin >= location.end) {
                throw std::runtime_error("Raft log: invalid location index");
            }
            locations_.push_back(location);
            if (!extents_.empty() && extents_.back().segment == location.segment && extents_.back().end == location.begin) {
                extents_.back().end = location.end;
            }
            else { extents_.push_back({location.segment, location.begin, location.end}); }
        }
        [[nodiscard]] bool empty() const noexcept { return locations_.empty(); }
        [[nodiscard]] size_t size() const noexcept { return locations_.size(); }
        [[nodiscard]] const Location& back() const { return locations_.back(); }
        [[nodiscard]] const auto& extents() const noexcept { return extents_; }
    private:
        std::deque<Location> locations_;
        std::deque<Extent> extents_;
    };
}

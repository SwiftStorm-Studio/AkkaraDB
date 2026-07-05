/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkaradb/include/akkaradb/detail/PTIndex.hpp
#pragma once

template <auto FieldPtr>
class Index {
    public:
        using Field = binpack::detail::memberOf<FieldPtr>;

        class FindRange {
            public:
                FindRange(FindRange&&) noexcept = default;
                FindRange& operator=(FindRange&&) noexcept = default;
                FindRange(const FindRange&) = delete;
                FindRange& operator=(const FindRange&) = delete;

                [[nodiscard]] bool hasNext() const noexcept { return pending_.has_value(); }

                [[nodiscard]] Entry next() {
                    if (!pending_) { throw std::out_of_range("PackedTable::Index::FindRange: no next entry"); }
                    Entry out = std::move(*pending_);
                    advance();
                    return out;
                }

            private:
                friend class Index;

                FindRange(PackedTable* table, size_t searchPrefixSize, std::span<const uint8_t> startKey, std::span<const uint8_t> endKey)
                    : table_{table},
                      searchPrefixSize_{searchPrefixSize},
                      scanArena_{std::make_unique<core::BufferArena>()},
                      rows_{table_->engine_->scan(*scanArena_, startKey, endKey)},
                      it_{rows_.begin()} { advance(); }

                void advance() {
                    pending_.reset();
                    while (!(it_ == rows_.end())) {
                        const auto& raw = *it_;
                        const auto key = raw.key;
                        if (key.size() <= searchPrefixSize_) {
                            ++it_;
                            continue;
                        }

                        std::span pkBytes{key.data() + searchPrefixSize_, key.size() - searchPrefixSize_};
                        Entry entry;
                        if (table_->getByPkBytes(pkBytes, entry)) {
                            pending_ = std::move(entry);
                            ++it_;
                            return;
                        }
                        ++it_;
                    }
                }

                PackedTable* table_;
                size_t searchPrefixSize_;
                std::unique_ptr<core::BufferArena> scanArena_;
                core::ArenaGenerator<engine::AkkEngine::ScanRecordView> rows_;
                core::ArenaGenerator<engine::AkkEngine::ScanRecordView>::iterator it_;
                std::optional<Entry> pending_;
        };

        [[nodiscard]] FindRange find(const Field& value) const {
            std::vector<uint8_t> fieldBytes;
            table_->encodeIndexFieldValue(value, fieldBytes);
            std::vector<uint8_t> startKey;
            table_->makeIndexSearchPrefix(prefix_, fieldBytes, startKey);
            std::vector<uint8_t> endKey = startKey;
            if (!detail::incrementLexicographicBytes(endKey.data(), endKey.size())) { endKey.clear(); }
            return FindRange{table_, startKey.size(), startKey, endKey};
        }

    private:
        friend class PackedTable;
        Index(PackedTable* table, std::array<uint8_t, 8> prefix) : table_{table}, prefix_{prefix} {}

        PackedTable* table_;
        std::array<uint8_t, 8> prefix_;
};

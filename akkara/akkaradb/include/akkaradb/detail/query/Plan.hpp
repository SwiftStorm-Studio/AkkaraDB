/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkaradb/include/akkaradb/detail/query/Plan.hpp
#pragma once

enum class QuerySourceKind {
    TABLE, INDEX
};

struct QueryRange {
    std::vector<uint8_t> startKey;
    std::vector<uint8_t> endKey;
    size_t indexSearchPrefixSize = 0;
    bool dynamicIndexPkOffset = false;
    bool prefixIndexPkOffset = false;
    bool dedupeIndexPks = false;
};

struct QueryPlan {
    QuerySourceKind kind = QuerySourceKind::TABLE;
    std::vector<QueryRange> ranges;
    int score = 0;
};

class QuerySource {
    public:
        QuerySource(QuerySource&&) noexcept = default;
        QuerySource& operator=(QuerySource&&) noexcept = default;
        QuerySource(const QuerySource&) = delete;
        QuerySource& operator=(const QuerySource&) = delete;

        [[nodiscard]] bool hasNext() const noexcept { return pending_.has_value(); }

        [[nodiscard]] Entry next() {
            if (!pending_) { throw std::out_of_range("PackedTable::QuerySource: no next entry"); }
            Entry out = std::move(*pending_);
            advance();
            return out;
        }

    private:
        friend class PackedTable;

        QuerySource(const PackedTable* table, QueryPlan plan)
            : table_{table}, kind_{plan.kind}, ranges_{std::move(plan.ranges)}, scanArena_{std::make_unique<core::BufferArena>()} {
            openNextRange();
            advance();
        }

        void openNextRange() {
            while (rangeIndex_ < ranges_.size()) {
                const auto& range = ranges_[rangeIndex_++];
                indexSearchPrefixSize_ = range.indexSearchPrefixSize;
                dynamicIndexPkOffset_ = range.dynamicIndexPkOffset;
                prefixIndexPkOffset_ = range.prefixIndexPkOffset;
                dedupeIndexPks_ = range.dedupeIndexPks;
                it_ = {};
                rows_ = {};
                scanArena_->reset();
                rows_ = table_->engine_->scan(*scanArena_, range.startKey, range.endKey);
                it_ = rows_.begin();
                return;
            }
        }

        void advance() {
            pending_.reset();
            if (ranges_.empty()) { return; }
            while (rangeIndex_ <= ranges_.size()) {
                while (!(it_ == rows_.end())) {
                    const auto& raw = *it_;
                    Entry entry;
                    if (kind_ == QuerySourceKind::TABLE) {
                        const auto key = raw.key;
                        if (key.size() < table_->pkPrefix_.size() || std::memcmp(
                            key.data(),
                            table_->pkPrefix_.data(),
                            table_->pkPrefix_.size()
                        ) != 0) { return; }

                        std::span<const uint8_t> pkBytes{key.data() + table_->pkPrefix_.size(), key.size() - table_->pkPrefix_.size()};
                        entry = Entry{table_->decodePrimaryKeyBytes(pkBytes), binpack::BinPack::decode<Entity>(raw.value)};
                        table_->attachRefBindings(entry.value);
                        table_->sealImmutableFields(entry.value);
                    }
                    else {
                        const auto key = raw.key;
                        size_t pkOffset = indexSearchPrefixSize_;
                        if (prefixIndexPkOffset_) {
                            const auto offset = table_->prefixIndexPkOffset(key, 8);
                            if (!offset.has_value()) {
                                ++it_;
                                continue;
                            }
                            pkOffset = *offset;
                        }
                        else if (dynamicIndexPkOffset_) {
                            if (key.size() <= 12) {
                                ++it_;
                                continue;
                            }
                            size_t fieldSize = readLe32(key.data() + 8);
                            if (12 + fieldSize >= key.size()) {
                                const size_t legacyFieldSize = readBe32(key.data() + 8);
                                if (12 + legacyFieldSize < key.size()) { fieldSize = legacyFieldSize; }
                            }
                            pkOffset = 12 + fieldSize;
                        }
                        if (key.size() <= pkOffset) {
                            ++it_;
                            continue;
                        }
                        const std::span<const uint8_t> pkBytes{key.data() + pkOffset, key.size() - pkOffset};
                        if (dedupeIndexPks_) {
                            std::string pkKey{reinterpret_cast<const char*>(pkBytes.data()), pkBytes.size()};
                            if (!seenIndexPks_.insert(std::move(pkKey)).second) {
                                ++it_;
                                continue;
                            }
                        }
                        if (!table_->getByPkBytes(pkBytes, entry)) {
                            ++it_;
                            continue;
                        }
                    }
                    pending_ = std::move(entry);
                    ++it_;
                    return;
                }
                if (rangeIndex_ >= ranges_.size()) { return; }
                openNextRange();
            }
        }

        const PackedTable* table_;
        QuerySourceKind kind_;
        size_t indexSearchPrefixSize_ = 0;
        bool dynamicIndexPkOffset_ = false;
        bool prefixIndexPkOffset_ = false;
        bool dedupeIndexPks_ = false;
        std::unordered_set<std::string> seenIndexPks_;
        std::vector<QueryRange> ranges_;
        size_t rangeIndex_ = 0;
        std::unique_ptr<core::BufferArena> scanArena_;
        core::ArenaGenerator<engine::AkkEngine::ScanRecordView> rows_;
        core::ArenaGenerator<engine::AkkEngine::ScanRecordView>::iterator it_;
        std::optional<Entry> pending_;
};

template <typename Expr>
class QueryView {
    public:
        class Iterator {
            public:
                struct Sentinel {};

                using value_type = Entry;
                using difference_type = std::ptrdiff_t;
                using iterator_category = std::input_iterator_tag;

                [[nodiscard]] const Entry& operator*() const noexcept { return *current_; }
                [[nodiscard]] const Entry* operator->() const noexcept { return &*current_; }

                Iterator& operator++() {
                    advance();
                    return *this;
                }

                [[nodiscard]] bool operator!=(const Sentinel&) const noexcept { return current_.has_value(); }
                [[nodiscard]] bool operator==(const Sentinel&) const noexcept { return !current_.has_value(); }

            private:
                friend class QueryView;

                Iterator(QuerySource source, Expr expr, std::optional<size_t> limit)
                    : source_{std::move(source)}, expr_{std::move(expr)}, limit_{limit} { advance(); }

                void advance() {
                    current_.reset();
                    while (source_.hasNext()) {
                        if (limit_ && matched_ >= *limit_) { return; }
                        auto entry = source_.next();
                        if (query::eval(expr_, entry.value)) {
                            current_ = std::move(entry);
                            ++matched_;
                            return;
                        }
                    }
                }

                QuerySource source_;
                Expr expr_;
                std::optional<size_t> limit_;
                size_t matched_ = 0;
                std::optional<Entry> current_;
        };

        [[nodiscard]] Iterator begin() const {
            QueryPlan plan;
            table_->makeQueryPlan(expr_, plan);
            return Iterator{QuerySource{table_, std::move(plan)}, expr_, limit_};
        }

        [[nodiscard]] typename Iterator::Sentinel end() const noexcept { return {}; }

        template <typename NextExpr> requires(query::isExpr<NextExpr>)
        [[nodiscard]] auto where(NextExpr&& nextExpr) const {
            using StoredNextExpr = std::remove_cvref_t<NextExpr>;
            return QueryView<query::Logical<query::Op::AND, Expr, StoredNextExpr>>{
                table_,
                query::Logical<query::Op::AND, Expr, StoredNextExpr>{expr_, std::forward<NextExpr>(nextExpr)},
                limit_
            };
        }

        #ifndef AKKARADB_QUERY_REWRITE_PASS
        template <typename Pred> requires(!query::isExpr<Pred>)
        [[nodiscard]] auto where(Pred&& predicate) const {
            auto next = std::forward<Pred>(predicate)(query::makeProxy<Entity>());
            using NextExpr = decltype(next);
            return QueryView<query::Logical<query::Op::AND, Expr, NextExpr>>{
                table_,
                query::Logical<query::Op::AND, Expr, NextExpr>{expr_, std::move(next)},
                limit_
            };
        }
        #else
        template <typename Pred> requires(!query::isExpr<Pred>)
        [[nodiscard]] QueryView where(Pred&&) const { return *this; }
        #endif

        [[nodiscard]] QueryView limit(size_t n) const {
            QueryView out{*this};
            out.limit_ = n;
            return out;
        }

        [[nodiscard]] std::optional<Entry> first() const {
            auto it = begin();
            if (it != end()) { return *it; }
            return std::nullopt;
        }

        [[nodiscard]] bool any() const { return first().has_value(); }

        [[nodiscard]] size_t count() const {
            size_t n = 0;
            auto it = begin();
            while (it != end()) {
                ++n;
                ++it;
            }
            return n;
        }

        [[nodiscard]] std::vector<Entry> toVector() const {
            std::vector<Entry> out;
            for (const auto& entry : *this) { out.push_back(entry); }
            return out;
        }

    private:
        friend class PackedTable;

        QueryView(const PackedTable* table, Expr expr, std::optional<size_t> limit = std::nullopt)
            : table_{table}, expr_{std::move(expr)}, limit_{limit} {}

        const PackedTable* table_;
        Expr expr_;
        std::optional<size_t> limit_;
};

[[nodiscard]] QueryView<query::AlwaysTrue> query() const { return QueryView<query::AlwaysTrue>{this, query::AlwaysTrue{}}; }

template <typename Expr> requires(query::isExpr<Expr>)
[[nodiscard]] auto query(Expr&& expr) const {
    using StoredExpr = std::remove_cvref_t<Expr>;
    return QueryView<StoredExpr>{this, std::forward<Expr>(expr)};
}

#ifndef AKKARADB_QUERY_REWRITE_PASS
template <typename Pred> requires(!query::isExpr<Pred>)
[[nodiscard]] auto query(Pred&& predicate) const { return query().where(std::forward<Pred>(predicate)); }
#else
template <typename Pred> requires(!query::isExpr<Pred>)
[[nodiscard]] QueryView<query::AlwaysTrue> query(Pred&&) const { return query(); }
#endif

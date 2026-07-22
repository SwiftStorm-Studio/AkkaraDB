/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkaradb/include/akkaradb/detail/query/BytecodeView.hpp
#pragma once

class BytecodeQueryView {
    public:
        using Prepared = query::bytecode::PreparedQuery<Entity>;

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
                friend class BytecodeQueryView;

                Iterator(const PackedTable* table, QueryPlan plan, Prepared query, std::optional<size_t> limit)
                    : table_{table}, plan_{std::move(plan)}, query_{std::move(query)}, limit_{limit}, raw_{query_.canEvalRaw()} {
                    if (raw_) {
                        rawArena_ = std::make_unique<core::BufferArena>();
                        openNextRawRange();
                    }
                    else { source_ = std::unique_ptr<QuerySource>(new QuerySource{table_, std::move(plan_)}); }
                    advance();
                }

                void advance() {
                    if (raw_) {
                        advanceRaw();
                        return;
                    }
                    advanceDecoded();
                }

                void advanceDecoded() {
                    current_.reset();
                    while (source_->hasNext()) {
                        if (limit_ && matched_ >= *limit_) { return; }

                        auto entry = source_->next();
                        if (query::bytecode::eval(query_, entry.value)) {
                            current_ = std::move(entry);
                            ++matched_;
                            return;
                        }
                    }
                }

                void openNextRawRange() {
                    while (rangeIndex_ < plan_.ranges.size()) {
                        const auto& range = plan_.ranges[rangeIndex_++];
                        rawIndexSearchPrefixSize_ = range.indexSearchPrefixSize;
                        rawDynamicIndexPkOffset_ = range.dynamicIndexPkOffset;
                        rawPrefixIndexPkOffset_ = range.prefixIndexPkOffset;
                        rawDedupeIndexPks_ = range.dedupeIndexPks;
                        rawRows_ = {};
                        rawArena_->reset();
                        rawRows_ = table_->engine_->scan(*rawArena_, range.startKey, range.endKey);
                        rawIt_ = rawRows_.begin();
                        return;
                    }
                }

                [[nodiscard]] bool rawValueByPkBytes(std::span<const uint8_t> pkBytes, std::span<const uint8_t>& valueBytes) {
                    rawLookupKey_.clear();
                    rawLookupKey_.reserve(table_->pkPrefix_.size() + pkBytes.size());
                    rawLookupKey_.insert(rawLookupKey_.end(), table_->pkPrefix_.begin(), table_->pkPrefix_.end());
                    rawLookupKey_.insert(rawLookupKey_.end(), pkBytes.begin(), pkBytes.end());
                    return table_->engine_->getIntoArena(rawLookupKey_, *rawArena_, valueBytes);
                }

                [[nodiscard]] std::optional<std::span<const uint8_t>> rawIndexPkBytes(std::span<const uint8_t> key) {
                    size_t pkOffset = rawIndexSearchPrefixSize_;
                    if (rawPrefixIndexPkOffset_) {
                        const auto offset = table_->prefixIndexPkOffset(key, 8);
                        if (!offset.has_value()) { return std::nullopt; }
                        pkOffset = *offset;
                    }
                    else if (rawDynamicIndexPkOffset_) {
                        if (key.size() <= 12) { return std::nullopt; }
                        size_t fieldSize = table_->readLe32(key.data() + 8);
                        if (12 + fieldSize >= key.size()) {
                            const size_t legacyFieldSize = table_->readBe32(key.data() + 8);
                            if (12 + legacyFieldSize < key.size()) { fieldSize = legacyFieldSize; }
                        }
                        pkOffset = 12 + fieldSize;
                    }
                    if (key.size() <= pkOffset) { return std::nullopt; }
                    return std::span<const uint8_t>{key.data() + pkOffset, key.size() - pkOffset};
                }

                void yieldRawMatch(std::span<const uint8_t> pkBytes, std::span<const uint8_t> valueBytes) {
                    Entry entry{table_->decodePrimaryKeyBytes(pkBytes), binpack::BinPack::decode<Entity>(valueBytes)};
                    table_->attachRefBindings(entry.value);
                    table_->sealImmutableFields(entry.value);
                    current_ = std::move(entry);
                    ++matched_;
                }

                void advanceRaw() {
                    current_.reset();
                    if (plan_.ranges.empty()) { return; }
                    while (rangeIndex_ <= plan_.ranges.size()) {
                        while (!(rawIt_ == rawRows_.end())) {
                            if (limit_ && matched_ >= *limit_) { return; }

                            const auto& raw = *rawIt_;
                            const auto key = raw.key;

                            if (plan_.kind == QuerySourceKind::TABLE) {
                                if (key.size() < table_->pkPrefix_.size() || std::memcmp(
                                    key.data(),
                                    table_->pkPrefix_.data(),
                                    table_->pkPrefix_.size()
                                ) != 0) { return; }

                                const std::span<const uint8_t> pkBytes{
                                    key.data() + table_->pkPrefix_.size(),
                                    key.size() - table_->pkPrefix_.size()
                                };
                                if (query::bytecode::evalRaw(query_, raw.value)) {
                                    yieldRawMatch(pkBytes, raw.value);
                                    ++rawIt_;
                                    return;
                                }
                                ++rawIt_;
                                continue;
                            }

                            const auto pkBytes = rawIndexPkBytes(key);
                            if (!pkBytes.has_value()) {
                                ++rawIt_;
                                continue;
                            }
                            if (rawDedupeIndexPks_) {
                                std::string pkKey{reinterpret_cast<const char*>(pkBytes->data()), pkBytes->size()};
                                if (!rawSeenIndexPks_.insert(std::move(pkKey)).second) {
                                    ++rawIt_;
                                    continue;
                                }
                            }

                            std::span<const uint8_t> valueBytes;
                            if (!rawValueByPkBytes(*pkBytes, valueBytes)) {
                                ++rawIt_;
                                continue;
                            }
                            if (query::bytecode::evalRaw(query_, valueBytes)) {
                                yieldRawMatch(*pkBytes, valueBytes);
                                ++rawIt_;
                                return;
                            }
                            ++rawIt_;
                        }
                        if (rangeIndex_ >= plan_.ranges.size()) { return; }
                        openNextRawRange();
                    }
                }

                const PackedTable* table_;
                QueryPlan plan_;
                std::unique_ptr<QuerySource> source_;
                Prepared query_;
                std::optional<size_t> limit_;
                bool raw_ = false;
                size_t matched_ = 0;
                std::optional<Entry> current_;
                size_t rangeIndex_ = 0;
                size_t rawIndexSearchPrefixSize_ = 0;
                bool rawDynamicIndexPkOffset_ = false;
                bool rawPrefixIndexPkOffset_ = false;
                bool rawDedupeIndexPks_ = false;
                std::unordered_set<std::string> rawSeenIndexPks_;
                std::vector<uint8_t> rawLookupKey_;
                std::unique_ptr<core::BufferArena> rawArena_;
                core::ArenaGenerator<engine::AkkEngine::ScanRecordView> rawRows_;
                core::ArenaGenerator<engine::AkkEngine::ScanRecordView>::iterator rawIt_;
        };

        template <typename Pred>
        class FilteredView {
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
                        friend class FilteredView;

                        Iterator(typename BytecodeQueryView::Iterator inner, typename BytecodeQueryView::Iterator::Sentinel end, Pred predicate)
                            : inner_{std::move(inner)}, end_{end}, predicate_{std::move(predicate)} {
                            advance();
                        }

                        void advance() {
                            current_.reset();
                            while (inner_ != end_) {
                                Entry entry = *inner_;
                                ++inner_;
                                if (predicate_(entry.value)) {
                                    current_ = std::move(entry);
                                    return;
                                }
                            }
                        }

                        typename BytecodeQueryView::Iterator inner_;
                        typename BytecodeQueryView::Iterator::Sentinel end_;
                        Pred predicate_;
                        std::optional<Entry> current_;
                };

                [[nodiscard]] Iterator begin() const { return Iterator{base_.begin(), base_.end(), predicate_}; }
                [[nodiscard]] typename Iterator::Sentinel end() const noexcept { return {}; }

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
                friend class BytecodeQueryView;

                FilteredView(BytecodeQueryView base, Pred predicate)
                    : base_{std::move(base)}, predicate_{std::move(predicate)} {}

                BytecodeQueryView base_;
                Pred predicate_;
        };

        [[nodiscard]] Iterator begin() const {
            QueryPlan plan;
            table_->makeBytecodeQueryPlan(query_, plan);
            return Iterator{table_, std::move(plan), query_, limit_};
        }

        [[nodiscard]] typename Iterator::Sentinel end() const noexcept { return {}; }

        [[nodiscard]] BytecodeQueryView limit(size_t n) const {
            BytecodeQueryView out{*this};
            out.limit_ = n;
            return out;
        }

        template <typename Pred>
        [[nodiscard]] auto where(Pred&& predicate) const {
            using StoredPred = std::remove_cvref_t<Pred>;
            return FilteredView<StoredPred>{*this, std::forward<Pred>(predicate)};
        }

        [[nodiscard]] BytecodeQueryView where(query::bytecode::CompiledQueryDescriptor<Entity> descriptor) const {
            Prepared next{descriptor};
            return BytecodeQueryView{
                table_,
                Prepared{query::bytecode::composeAnd(query_.descriptor(), next.descriptor())},
                limit_
            };
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

        BytecodeQueryView(const PackedTable* table, Prepared query, std::optional<size_t> limit = std::nullopt)
            : table_{table}, query_{std::move(query)}, limit_{limit} {}

        const PackedTable* table_;
        Prepared query_;
        std::optional<size_t> limit_;
};

[[nodiscard]] BytecodeQueryView __query_compiled(query::bytecode::CompiledQueryDescriptor<Entity> descriptor) const {
    return BytecodeQueryView{this, query::bytecode::PreparedQuery<Entity>{descriptor}};
}

[[nodiscard]] BytecodeQueryView query(query::bytecode::CompiledQueryDescriptor<Entity> descriptor) const {
    return __query_compiled(descriptor);
}

void makeBytecodeQueryPlan(const query::bytecode::PreparedQuery<Entity>& query, QueryPlan& plan) const {
    resetTempBuffers();
    const auto& descriptor = query.descriptor();
    QueryPlan bestPlan;
    bool hasBestPlan = false;
    for (const auto& hint : descriptor.planHints) {
        QueryPlan hintPlan;
        if (!tryMakeBytecodeQueryPlanHint(descriptor, hint, hintPlan)) { continue; }
        if (!hasBestPlan) {
            bestPlan = std::move(hintPlan);
            hasBestPlan = true;
            continue;
        }

        QueryPlan intersection;
        if (tryIntersectIndexPlans(bestPlan, hintPlan, intersection)) { bestPlan = std::move(intersection); }
        else if (hintPlan.score > bestPlan.score) { bestPlan = std::move(hintPlan); }
    }
    if (hasBestPlan) {
        plan = std::move(bestPlan);
        return;
    }

    plan.kind = QuerySourceKind::TABLE;
    plan.ranges.clear();
    plan.score = 0;
    makePrefixStartEnd(pkPrefix_, scanStartBuffer_, scanEndBuffer_);
    addQueryRange(plan, scanStartBuffer_, scanEndBuffer_, 0);
}

bool tryMakeBytecodeQueryPlanHint(
    const query::bytecode::CompiledQueryDescriptor<Entity>& descriptor,
    const query::bytecode::PlanHint& hint,
    QueryPlan& plan
) const {
    if (hint.field >= descriptor.fields.size() || hint.constant >= descriptor.constants.size()) { return false; }

    const auto& field = descriptor.fields[hint.field];
    if (field.name.empty() || field.encodeIndexValue == nullptr) { return false; }

    const IndexDef* index = nullptr;
    for (const auto& candidate : indexes_) {
        if (candidate.fieldName == field.name) {
            index = &candidate;
            break;
        }
    }

    std::vector<uint8_t> encodedField;

    using query::bytecode::PlanHintOp;
    if (hint.op == PlanHintOp::StartsWith) {
        const auto& constant = descriptor.constants[hint.constant];
        if (constant.kind != query::bytecode::ValueKind::String) { return false; }

        const PrefixIndexDef* prefixIndex = nullptr;
        for (const auto& candidate : prefixIndexes_) {
            if (candidate.fieldName == field.name) {
                prefixIndex = &candidate;
                break;
            }
        }
        if (prefixIndex == nullptr) { return false; }

        fieldBuffer_.clear();
        encodePrefixIndexStringSegment(constant.s, fieldBuffer_, false);
        makePrefixIndexSearchPrefix(prefixIndex->prefix, fieldBuffer_, scanStartBuffer_);
        scanEndBuffer_ = scanStartBuffer_;
        if (!detail::incrementLexicographicBytes(scanEndBuffer_.data(), scanEndBuffer_.size())) { scanEndBuffer_.clear(); }

        plan.kind = QuerySourceKind::INDEX;
        plan.ranges.clear();
        plan.score = queryPlanScoreStringPrefix;
        addQueryRange(plan, scanStartBuffer_, scanEndBuffer_, 0, false, true, false);
        return true;
    }

    if (index == nullptr) { return false; }
    if (field.encodeIndexValue == nullptr || !field.encodeIndexValue(descriptor.constants[hint.constant], encodedField)) { return false; }

    if (hint.op == PlanHintOp::Eq) {
        makeIndexSearchPrefix(index->prefix, encodedField, scanStartBuffer_);
        scanEndBuffer_ = scanStartBuffer_;
        if (!detail::incrementLexicographicBytes(scanEndBuffer_.data(), scanEndBuffer_.size())) { scanEndBuffer_.clear(); }

        plan.kind = QuerySourceKind::INDEX;
        plan.ranges.clear();
        plan.score = queryPlanScoreEquality;
        addQueryRange(plan, scanStartBuffer_, scanEndBuffer_, scanStartBuffer_.size());
        return true;
    }

    if (!field.orderedIndexable) { return false; }

    makeIndexSearchPrefix(index->prefix, encodedField, indexKeyBuffer_);
    const size_t pkOffset = indexKeyBuffer_.size();
    makePrefixStartEnd(index->prefix, scanStartBuffer_, scanEndBuffer_);

    if (hint.op == PlanHintOp::Gt) {
        scanStartBuffer_ = indexKeyBuffer_;
        if (!detail::incrementLexicographicBytes(scanStartBuffer_.data(), scanStartBuffer_.size())) { scanStartBuffer_.clear(); }
    }
    else if (hint.op == PlanHintOp::Ge) { scanStartBuffer_ = indexKeyBuffer_; }
    else if (hint.op == PlanHintOp::Lt) { scanEndBuffer_ = indexKeyBuffer_; }
    else if (hint.op == PlanHintOp::Le) {
        scanEndBuffer_ = indexKeyBuffer_;
        if (!detail::incrementLexicographicBytes(scanEndBuffer_.data(), scanEndBuffer_.size())) { scanEndBuffer_.clear(); }
    }
    else { return false; }

    plan.kind = QuerySourceKind::INDEX;
    plan.ranges.clear();
    plan.score = queryPlanScoreOrderedRange;
    addQueryRange(plan, scanStartBuffer_, scanEndBuffer_, pkOffset);
    return true;
}

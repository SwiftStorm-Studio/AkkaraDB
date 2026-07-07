/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkaradb/include/akkaradb/detail/query/Optimize.hpp
#pragma once

static constexpr int queryPlanScoreFullFieldIndex = 10;
static constexpr int queryPlanScoreOrderedRange = 80;
static constexpr int queryPlanScoreNullEquality = 85;
static constexpr int queryPlanScoreInEquality = 90;
static constexpr int queryPlanScoreStringPrefix = 95;
static constexpr int queryPlanScoreEquality = 100;
static constexpr int queryPlanScoreOrUnion = 70;

template <typename Expr>
void makeQueryPlan(const Expr& expr, QueryPlan& plan) const {
    resetTempBuffers();
    if (tryMakeIndexPlan(expr, plan)) { return; }

    plan.kind = QuerySourceKind::TABLE;
    plan.ranges.clear();
    plan.score = 0;
    makePrefixStartEnd(pkPrefix_, scanStartBuffer_, scanEndBuffer_);
    addQueryRange(plan, scanStartBuffer_, scanEndBuffer_, 0);
}

void addQueryRange(
    QueryPlan& plan,
    std::span<const uint8_t> startKey,
    std::span<const uint8_t> endKey,
    size_t indexSearchPrefixSize,
    bool dynamicIndexPkOffset = false,
    bool prefixIndexPkOffset = false,
    bool dedupeIndexPks = false
) const {
    QueryRange range;
    range.startKey.assign(startKey.begin(), startKey.end());
    range.endKey.assign(endKey.begin(), endKey.end());
    range.indexSearchPrefixSize = indexSearchPrefixSize;
    range.dynamicIndexPkOffset = dynamicIndexPkOffset;
    range.prefixIndexPkOffset = prefixIndexPkOffset;
    range.dedupeIndexPks = dedupeIndexPks;
    plan.ranges.push_back(std::move(range));
}

template <typename Expr>
[[nodiscard]] bool tryMakeIndexPlan(const Expr& expr, QueryPlan& plan) const {
    using E = std::remove_cvref_t<Expr>;
    if constexpr (query::isAnd<E>) {
        QueryPlan lhsPlan;
        QueryPlan rhsPlan;
        const bool lhsUsable = tryMakeIndexPlan(expr.lhs, lhsPlan);
        const bool rhsUsable = tryMakeIndexPlan(expr.rhs, rhsPlan);
        if (!lhsUsable && !rhsUsable) { return false; }
        if (!rhsUsable || (lhsUsable && lhsPlan.score >= rhsPlan.score)) { plan = std::move(lhsPlan); }
        else { plan = std::move(rhsPlan); }
        return true;
    }
    else if constexpr (query::isOr<E>) {
        QueryPlan lhsPlan;
        QueryPlan rhsPlan;
        const bool lhsUsable = tryMakeIndexPlan(expr.lhs, lhsPlan);
        const bool rhsUsable = tryMakeIndexPlan(expr.rhs, rhsPlan);
        if (!lhsUsable || !rhsUsable) { return false; }

        plan.kind = QuerySourceKind::INDEX;
        plan.ranges = std::move(lhsPlan.ranges);
        plan.ranges.insert(
            plan.ranges.end(),
            std::make_move_iterator(rhsPlan.ranges.begin()),
            std::make_move_iterator(rhsPlan.ranges.end())
        );
        for (auto& range : plan.ranges) { range.dedupeIndexPks = true; }

        const int lowerScore = lhsPlan.score < rhsPlan.score ? lhsPlan.score : rhsPlan.score;
        plan.score = lowerScore < queryPlanScoreOrUnion ? lowerScore : queryPlanScoreOrUnion;
        return true;
    }
    else if constexpr (query::isCompare<E>) { return tryMakeCompareIndexPlan(expr, plan); }
    else if constexpr (query::isNot<E>) { return tryMakeNotIndexPlan(expr, plan); }
    else if constexpr (query::isUnary<E>) { return tryMakeUnaryIndexPlan(expr, plan); }
    else { return false; }
}

template <query::Op Operator, typename L, typename R>
[[nodiscard]] bool tryMakeCompareIndexPlan(const query::Compare<Operator, L, R>& expr, QueryPlan& plan) const {
    if constexpr (query::isColumn<L>&& query::isLiteral<R>) {
        return tryMakeFieldIndexPlan < Operator, std::remove_cvref_t<L>::fieldPtr > (query::literalValue(expr.rhs), plan);
    }
    else if constexpr (query::isLiteral<L>&& query::isColumn<R>) {
        return tryMakeFieldIndexPlan < query::swappedCompareOp < Operator >, std::remove_cvref_t<R>::fieldPtr > (query::literalValue(
            expr.lhs
        ), plan);
    }
    else { return false; }
}

template <typename X>
[[nodiscard]] bool tryMakeNotIndexPlan(const query::Not<X>& expr, QueryPlan& plan) const {
    using E = std::remove_cvref_t<X>;
    if constexpr (query::isCompare<E>) { return tryMakeCompareIndexSourcePlan(expr.x, plan); }
    else if constexpr (query::isUnary<E>) { return tryMakeUnaryIndexPlan(expr.x, plan); }
    else { return false; }
}

template <query::Op Operator, typename L, typename R>
[[nodiscard]] bool tryMakeCompareIndexSourcePlan(const query::Compare<Operator, L, R>&, QueryPlan& plan) const {
    if constexpr (query::isColumn<L>&& query::isLiteral<R>) { return tryMakeFieldIndexSourcePlan<std::remove_cvref_t<L>::fieldPtr>(plan); }
    else if constexpr (query::isLiteral<L>&& query::isColumn<R>) {
        return tryMakeFieldIndexSourcePlan<std::remove_cvref_t<R>::fieldPtr>(plan);
    }
    else { return false; }
}

template <query::Op Operator, typename X>
[[nodiscard]] bool tryMakeUnaryIndexPlan(const query::Unary<Operator, X>&, QueryPlan& plan) const {
    if constexpr ((Operator == query::Op::IS_NULL || Operator == query::Op::IS_NOT_NULL) && query::isColumn<X>) {
        return tryMakeNullIndexPlan < Operator, std::remove_cvref_t<X>::fieldPtr > (plan);
    }
    else { return false; }
}

template <auto FieldPtr>
[[nodiscard]] const IndexDef* findIndexDefFor() const {
    const std::string_view fieldName = binpack::detail::memberName<FieldPtr>();
    for (const auto& idx : indexes_) { if (idx.fieldName == fieldName) { return &idx; } }
    return nullptr;
}

template <auto FieldPtr>
[[nodiscard]] const PrefixIndexDef* findPrefixIndexDefFor() const {
    const std::string_view fieldName = binpack::detail::memberName<FieldPtr>();
    for (const auto& idx : prefixIndexes_) { if (idx.fieldName == fieldName) { return &idx; } }
    return nullptr;
}

template <typename Field, typename Lit>
[[nodiscard]] static bool literalToField(const Lit& literal, Field& out) {
    if constexpr (std::is_same_v<Field, std::string>) {
        if constexpr (std::is_convertible_v<Lit, std::string_view>) {
            out = std::string{std::string_view{literal}};
            return true;
        }
        else { return false; }
    }
    else if constexpr (std::is_arithmetic_v<Field>&& std::is_arithmetic_v<Lit>) {
        if constexpr (std::is_unsigned_v<Field>&& std::is_signed_v<Lit>) { if (literal < 0) { return false; } }
        const auto value = static_cast<long double>(literal);
        if (value < static_cast<long double>(std::numeric_limits<Field>::lowest()) || value > static_cast<long double>(std::numeric_limits<
            Field>::max())) { return false; }
        out = static_cast<Field>(literal);
        return true;
    }
    else if constexpr (std::is_constructible_v<Field, Lit>) {
        out = Field{literal};
        return true;
    }
    else if constexpr (std::is_convertible_v<Lit, Field>) {
        out = literal;
        return true;
    }
    else { return false; }
}

[[nodiscard]] static bool likePatternToPrefix(std::string_view pattern, std::string_view& prefix) {
    const size_t wildcard = pattern.find_first_of("%_");
    if (wildcard == std::string_view::npos) {
        prefix = pattern;
        return true;
    }
    if (pattern[wildcard] != '%' || wildcard + 1 != pattern.size()) { return false; }
    prefix = pattern.substr(0, wildcard);
    return true;
}

template <auto FieldPtr>
[[nodiscard]] bool tryMakePrefixIndexPlan(std::string_view prefix, QueryPlan& plan) const {
    const PrefixIndexDef* idx = findPrefixIndexDefFor<FieldPtr>();
    if (idx == nullptr) { return false; }

    fieldBuffer_.clear();
    encodePrefixIndexStringSegment(prefix, fieldBuffer_, false);
    makePrefixIndexSearchPrefix(idx->prefix, fieldBuffer_, scanStartBuffer_);
    scanEndBuffer_ = scanStartBuffer_;
    if (!detail::incrementLexicographicBytes(scanEndBuffer_.data(), scanEndBuffer_.size())) { scanEndBuffer_.clear(); }

    plan.kind = QuerySourceKind::INDEX;
    plan.ranges.clear();
    plan.score = queryPlanScoreStringPrefix;
    addQueryRange(plan, scanStartBuffer_, scanEndBuffer_, 0, false, true, false);
    return true;
}

[[nodiscard]] bool tryMakeFullFieldIndexPlan(
    const std::array<uint8_t, 8>& indexPrefix,
    QueryPlan& plan,
    int score = queryPlanScoreFullFieldIndex
) const {
    makePrefixStartEnd(indexPrefix, scanStartBuffer_, scanEndBuffer_);
    plan.kind = QuerySourceKind::INDEX;
    plan.ranges.clear();
    plan.score = score;
    addQueryRange(plan, scanStartBuffer_, scanEndBuffer_, 0, true, false, true);
    return true;
}

template <auto FieldPtr>
[[nodiscard]] bool tryMakeFieldIndexSourcePlan(QueryPlan& plan) const {
    const IndexDef* idx = findIndexDefFor<FieldPtr>();
    if (idx == nullptr) { return false; }
    return tryMakeFullFieldIndexPlan(idx->prefix, plan);
}

template <typename Field>
void addEqualityIndexRange(const IndexDef& idx, const Field& value, QueryPlan& plan, bool dedupeIndexPks = false) const {
    fieldBuffer_.clear();
    encodeIndexFieldValue(value, fieldBuffer_);
    makeIndexSearchPrefix(idx.prefix, fieldBuffer_, scanStartBuffer_);
    scanEndBuffer_ = scanStartBuffer_;
    if (!detail::incrementLexicographicBytes(scanEndBuffer_.data(), scanEndBuffer_.size())) { scanEndBuffer_.clear(); }
    addQueryRange(plan, scanStartBuffer_, scanEndBuffer_, scanStartBuffer_.size(), false, false, dedupeIndexPks);
}

template <query::Op Operator, typename Field>
void addOrderedIndexRange(const IndexDef& idx, const Field& value, QueryPlan& plan) const {
    fieldBuffer_.clear();
    encodeIndexFieldValue(value, fieldBuffer_);

    ArenaByteBuffer boundary{tempArena_.get()};
    makeIndexSearchPrefix(idx.prefix, fieldBuffer_, boundary);
    const size_t pkOffset = boundary.size();

    makePrefixStartEnd(idx.prefix, scanStartBuffer_, scanEndBuffer_);

    if constexpr (Operator == query::Op::GT) {
        scanStartBuffer_ = boundary;
        if (!detail::incrementLexicographicBytes(scanStartBuffer_.data(), scanStartBuffer_.size())) { scanStartBuffer_.clear(); }
    }
    else if constexpr (Operator == query::Op::GE) { scanStartBuffer_ = boundary; }
    else if constexpr (Operator == query::Op::LT) { scanEndBuffer_ = boundary; }
    else if constexpr (Operator == query::Op::LE) {
        scanEndBuffer_ = boundary;
        if (!detail::incrementLexicographicBytes(scanEndBuffer_.data(), scanEndBuffer_.size())) { scanEndBuffer_.clear(); }
    }

    addQueryRange(plan, scanStartBuffer_, scanEndBuffer_, pkOffset);
}

template <typename Field>
static constexpr bool orderedIndexRangeSupportedV = (std::is_arithmetic_v<Field> && !std::is_same_v<Field, bool>);

template <typename Field, typename Values>
[[nodiscard]] bool tryMakeInIndexPlan(const IndexDef& idx, const Values& values, QueryPlan& plan) const {
    if constexpr (requires { std::begin(values); std::end(values); }) {
        plan.kind = QuerySourceKind::INDEX;
        plan.ranges.clear();
        plan.score = queryPlanScoreInEquality;
        for (const auto& literal : values) {
            Field value{};
            if (!literalToField<Field>(literal, value)) { continue; }
            addEqualityIndexRange(idx, value, plan, true);
        }
        return true;
    }
    else { return false; }
}

template <query::Op Operator, auto FieldPtr>
[[nodiscard]] bool tryMakeNullIndexPlan(QueryPlan& plan) const {
    using Field = binpack::detail::memberOf<FieldPtr>;
    if constexpr (!query::isOptional<Field>) { return false; }
    else {
        const IndexDef* idx = findIndexDefFor<FieldPtr>();
        if (idx == nullptr) { return false; }

        if constexpr (Operator == query::Op::IS_NULL) {
            plan.kind = QuerySourceKind::INDEX;
            plan.ranges.clear();
            plan.score = queryPlanScoreNullEquality;
            addEqualityIndexRange(*idx, Field{}, plan);
            return true;
        }
        else { return tryMakeFullFieldIndexPlan(idx->prefix, plan); }
    }
}

template <query::Op Operator, auto FieldPtr, typename Lit>
[[nodiscard]] bool tryMakeFieldIndexPlan(const Lit& literal, QueryPlan& plan) const {
    using Field = binpack::detail::memberOf<FieldPtr>;
    const IndexDef* idx = findIndexDefFor<FieldPtr>();

    if constexpr (Operator == query::Op::STARTS_WITH) {
        if constexpr (query::isStringLikeV<Field>&& query::isStringLikeV<Lit>) {
            if (tryMakePrefixIndexPlan<FieldPtr>(std::string_view{literal}, plan)) { return true; }
            return idx != nullptr && tryMakeFullFieldIndexPlan(idx->prefix, plan);
        }
        else { return false; }
    }
    else if constexpr (Operator == query::Op::LIKE) {
        if constexpr (query::isStringLikeV<Field>&& query::isStringLikeV<Lit>) {
            const std::string_view pattern{literal};
            if (pattern.find_first_of("%_") != std::string_view::npos) {
                std::string_view prefix;
                if (likePatternToPrefix(pattern, prefix)) {
                    if (tryMakePrefixIndexPlan<FieldPtr>(prefix, plan)) { return true; }
                }
                return idx != nullptr && tryMakeFullFieldIndexPlan(idx->prefix, plan);
            }
            if (idx == nullptr) { return false; }

            Field value{};
            if (!literalToField<Field>(pattern, value)) { return false; }
            plan.kind = QuerySourceKind::INDEX;
            plan.ranges.clear();
            plan.score = queryPlanScoreEquality;
            addEqualityIndexRange(*idx, value, plan);
            return true;
        }
        else { return false; }
    }
    else if constexpr (Operator == query::Op::EQ) {
        if (idx == nullptr) { return false; }
        Field value{};
        if (!literalToField<Field>(literal, value)) { return false; }
        plan.kind = QuerySourceKind::INDEX;
        plan.ranges.clear();
        plan.score = queryPlanScoreEquality;
        addEqualityIndexRange(*idx, value, plan);
        return true;
    }
    else if constexpr (Operator == query::Op::NE) {
        if (idx == nullptr) { return false; }
        Field value{};
        if (!literalToField<Field>(literal, value)) { return false; }
        return tryMakeFullFieldIndexPlan(idx->prefix, plan);
    }
    else if constexpr (Operator == query::Op::IN_LIST) {
        if (idx == nullptr) { return false; }
        return tryMakeInIndexPlan<Field>(*idx, literal, plan);
    }
    else if constexpr (Operator == query::Op::NOT_IN) {
        if (idx == nullptr) { return false; }
        if constexpr (requires { std::begin(literal); std::end(literal); }) { return tryMakeFullFieldIndexPlan(idx->prefix, plan); }
        else { return false; }
    }
    else if constexpr (Operator == query::Op::CONTAINS) {
        if (idx == nullptr) { return false; }
        if constexpr (query::isStringLikeV<Field>&& query::isStringLikeV<Lit>) { return tryMakeFullFieldIndexPlan(idx->prefix, plan); }
        else { return false; }
    }
    else if constexpr (Operator == query::Op::GT || Operator == query::Op::GE || Operator == query::Op::LT || Operator == query::Op::LE) {
        if (idx == nullptr) { return false; }
        if constexpr (orderedIndexRangeSupportedV<Field>) {
            Field value{};
            if (!literalToField<Field>(literal, value)) { return false; }
            plan.kind = QuerySourceKind::INDEX;
            plan.ranges.clear();
            plan.score = queryPlanScoreOrderedRange;
            addOrderedIndexRange<Operator>(*idx, value, plan);
            return true;
        }
        else { return false; }
    }
    else { return false; }
}

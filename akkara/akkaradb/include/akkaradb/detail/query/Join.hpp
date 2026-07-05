/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkaradb/include/akkaradb/detail/query/Join.hpp
#pragma once

template <auto LeftFieldPtr, auto RightFieldPtr, auto TargetPrimaryKeyPtr>
class JoinView {
    public:
        using LeftEntry = Entry;
        using RightTable = PackedTable<TargetPrimaryKeyPtr>;
        using RightEntry = typename RightTable::Entry;
        using RightEntity = typename RightTable::Entity;
        using RightPK = typename RightTable::PK;

        struct JoinRow {
            LeftEntry left;
            RightEntity right;
        };

        class Iterator {
            public:
                struct Sentinel {};

                using value_type = JoinRow;
                using difference_type = std::ptrdiff_t;
                using iterator_category = std::input_iterator_tag;

                [[nodiscard]] const JoinRow& operator*() const noexcept { return *current_; }
                [[nodiscard]] const JoinRow* operator->() const noexcept { return &*current_; }

                Iterator& operator++() {
                    advance();
                    return *this;
                }

                [[nodiscard]] bool operator!=(const Sentinel&) const noexcept { return current_.has_value(); }
                [[nodiscard]] bool operator==(const Sentinel&) const noexcept { return !current_.has_value(); }

            private:
                friend class JoinView;

                explicit Iterator(const JoinView* view) : view_{view}, scan_{view_->left_->scanAll()} { advance(); }

                void advance() {
                    current_.reset();
                    if constexpr (usesRightPrimaryKey()) {
                        while (scan_.hasNext()) {
                            auto left = scan_.next();
                            std::optional<RightEntity> right;
                            if constexpr (isRef<binpack::detail::memberOf<LeftFieldPtr>>) {
                                right = view_->right_->getByRowId((left.value.*LeftFieldPtr).rowId());
                            }
                            else { right = view_->right_->get(view_->joinKey(left.value.*LeftFieldPtr)); }
                            if (!right) { continue; }
                            if (!view_->predicate_(left.value, *right)) { continue; }
                            current_ = JoinRow{std::move(left), std::move(*right)};
                            return;
                        }
                    }
                    else {
                        while (true) {
                            if (!left_) {
                                if (!scan_.hasNext()) { return; }
                                left_ = scan_.next();
                                rightScan_.emplace(view_->right_->scanAll());
                            }

                            while (rightScan_->hasNext()) {
                                auto right = rightScan_->next();
                                if (!view_->joinFieldsEqual(left_->value, right.value)) { continue; }
                                if (!view_->predicate_(left_->value, right.value)) { continue; }
                                current_ = JoinRow{*left_, std::move(right.value)};
                                return;
                            }

                            left_.reset();
                            rightScan_.reset();
                        }
                    }
                }

                const JoinView* view_;
                ScanRange scan_;
                std::optional<LeftEntry> left_;
                std::optional<typename RightTable::ScanRange> rightScan_;
                std::optional<JoinRow> current_;
        };

        [[nodiscard]] Iterator begin() const { return Iterator{this}; }
        [[nodiscard]] typename Iterator::Sentinel end() const noexcept { return {}; }

        template <typename Pred>
        [[nodiscard]] JoinView where(Pred&& predicate) const {
            auto previous = predicate_;
            auto next = std::function < bool(const Entity &, const RightEntity &) >
            {
                [previous = std::move(previous), predicate = std::forward<Pred>(predicate)](
                    const Entity& left,
                    const RightEntity& right
                ) mutable {
                    return previous(left, right) && predicate(left, right);
                }
            };
            return JoinView{left_, right_, std::move(next)};
        }

        [[nodiscard]] std::optional<JoinRow> first() const {
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

        [[nodiscard]] std::vector<JoinRow> toVector() const {
            std::vector<JoinRow> out;
            for (const auto& row : *this) { out.push_back(row); }
            return out;
        }

    private:
        friend class PackedTable;

        JoinView(
            const PackedTable* left,
            const RightTable* right,
            std::function<bool(const Entity &, const RightEntity &)> predicate = [](const Entity&, const RightEntity&) { return true; }
        ) : left_{left}, right_{right}, predicate_{std::move(predicate)} {}

        template <typename X>
        static decltype(auto) joinKey(const X& value) {
            using Field = std::remove_cvref_t<X>;
            if constexpr (isRef<Field>) { return value.id(); }
            else { return (value); }
        }

        static consteval bool usesRightPrimaryKey() {
            if constexpr (std::is_same_v<decltype(RightFieldPtr), decltype(TargetPrimaryKeyPtr)>) {
                return RightFieldPtr == TargetPrimaryKeyPtr;
            }
            else { return false; }
        }

        [[nodiscard]] bool joinFieldsEqual(const Entity& left, const RightEntity& right) const {
            return joinKey(left.*LeftFieldPtr) == joinKey(right.*RightFieldPtr);
        }

        const PackedTable* left_;
        const RightTable* right_;
        std::function<bool(const Entity &, const RightEntity &)> predicate_;
};

template <auto LeftFieldPtr, auto RightFieldPtr, auto TargetPrimaryKeyPtr>
[[nodiscard]] JoinView<LeftFieldPtr, RightFieldPtr, TargetPrimaryKeyPtr> join(const PackedTable<TargetPrimaryKeyPtr>& target) const {
    static_assert(std::is_same_v < binpack::detail::classOf < LeftFieldPtr >,
    Entity >, "join left field must belong to the left table entity"
    )
    ;
    using TargetTable = PackedTable<TargetPrimaryKeyPtr>;
    using RightEntity = typename TargetTable::Entity;
    static_assert(std::is_same_v < binpack::detail::classOf < RightFieldPtr >,
    RightEntity >, "join right field must belong to the right table entity"
    )
    ;
    using LeftField = binpack::detail::memberOf<LeftFieldPtr>;
    using RightField = binpack::detail::memberOf<RightFieldPtr>;
    static_assert(
        requires(const LeftField& left, const RightField& right) {
            {
                JoinView<LeftFieldPtr, RightFieldPtr, TargetPrimaryKeyPtr>::joinKey(left) == JoinView<LeftFieldPtr, RightFieldPtr,
                    TargetPrimaryKeyPtr>::joinKey(right)
            } -> std::convertible_to<bool>;
        },
        "join fields must be comparable"
    );
    return JoinView<LeftFieldPtr, RightFieldPtr, TargetPrimaryKeyPtr>{this, &target};
}

template <auto RefFieldPtr, auto TargetPrimaryKeyPtr> requires(isRef<binpack::detail::memberOf<RefFieldPtr>>)
[[nodiscard]] auto join(const PackedTable<TargetPrimaryKeyPtr>& target) const {
    static_assert(std::is_same_v < binpack::detail::classOf < RefFieldPtr >,
    Entity >, "join ref field must belong to the table entity"
    )
    ;
    using Field = binpack::detail::memberOf<RefFieldPtr>;
    using Target = typename RefTarget<Field>::Type;
    using TargetTable = PackedTable<TargetPrimaryKeyPtr>;
    static_assert(std::is_same_v<typename TargetTable::Entity, Target>, "join target table entity does not match Ref<T>") ;
    static_assert(std::is_same_v<typename Field::Key, typename TargetTable::PK>, "join key type does not match target table primary key");
    return join<RefFieldPtr, TargetPrimaryKeyPtr, TargetPrimaryKeyPtr>(target);
}

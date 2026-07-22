/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "TestErrorHandlers.hpp"

#include "akkaradb/AkkaraDB.hpp"

#include <cstdint>
#include <cstdio>
#include <vector>

namespace {
    struct PlannerRecord {
        uint64_t id;
        uint32_t age;
        uint32_t group;
    };

    AKKARADB_ENTITY(PlannerRecord, id, age, group)

    void testAndRangeIntersection() {
        auto db = akkaradb::AkkaraDB::open({}, akkaradb::StartupMode::ULTRA_FAST);
        auto table = db->table<&PlannerRecord::id>("planner_and");
        table.indexed<&PlannerRecord::age>();

        for (uint32_t i = 0; i < 100; ++i) { table.put(PlannerRecord{i, i, i % 3}); }

        const auto expr = akkaradb::query::Column<&PlannerRecord::age>{} >= 20U
            && akkaradb::query::Column<&PlannerRecord::age>{} < 30U;
        const auto rows = table.query(expr).toVector();
        AKK_TEST_CHECK(rows.size() == 10);
        for (const auto& row : rows) {
            AKK_TEST_CHECK(row.value.age >= 20U);
            AKK_TEST_CHECK(row.value.age < 30U);
        }
    }

    void testAndContradiction() {
        auto db = akkaradb::AkkaraDB::open({}, akkaradb::StartupMode::ULTRA_FAST);
        auto table = db->table<&PlannerRecord::id>("planner_and_contradiction");
        table.indexed<&PlannerRecord::age>();

        for (uint32_t i = 0; i < 100; ++i) { table.put(PlannerRecord{i, i, i % 3}); }

        const auto expr = akkaradb::query::Column<&PlannerRecord::age>{} == 5U
            && akkaradb::query::Column<&PlannerRecord::age>{} == 6U;
        const auto rows = table.query(expr).toVector();
        AKK_TEST_CHECK(rows.empty());
    }

    void testAndInRangeIntersection() {
        auto db = akkaradb::AkkaraDB::open({}, akkaradb::StartupMode::ULTRA_FAST);
        auto table = db->table<&PlannerRecord::id>("planner_and_in_range");
        table.indexed<&PlannerRecord::age>();

        for (uint32_t i = 0; i < 100; ++i) { table.put(PlannerRecord{i, i, i % 3}); }

        const auto expr = akkaradb::query::Column<&PlannerRecord::age>{}.in({10U, 20U, 30U})
            && akkaradb::query::Column<&PlannerRecord::age>{} >= 20U
            && akkaradb::query::Column<&PlannerRecord::age>{} < 30U;
        const auto rows = table.query(expr).toVector();
        AKK_TEST_CHECK(rows.size() == 1);
        AKK_TEST_CHECK(rows.front().value.age == 20U);
    }

    void testOrRangeMerge() {
        auto db = akkaradb::AkkaraDB::open({}, akkaradb::StartupMode::ULTRA_FAST);
        auto table = db->table<&PlannerRecord::id>("planner_or");
        table.indexed<&PlannerRecord::age>();

        for (uint32_t i = 0; i < 100; ++i) { table.put(PlannerRecord{i, i, i % 3}); }

        const auto expr = akkaradb::query::Column<&PlannerRecord::age>{} < 20U
            || akkaradb::query::Column<&PlannerRecord::age>{} < 30U;
        const auto rows = table.query(expr).toVector();
        AKK_TEST_CHECK(rows.size() == 30);
        for (const auto& row : rows) { AKK_TEST_CHECK(row.value.age < 30U); }
    }

    void testOrInDedupe() {
        auto db = akkaradb::AkkaraDB::open({}, akkaradb::StartupMode::ULTRA_FAST);
        auto table = db->table<&PlannerRecord::id>("planner_or_in_dedupe");
        table.indexed<&PlannerRecord::age>();

        for (uint32_t i = 0; i < 100; ++i) { table.put(PlannerRecord{i, i, i % 3}); }

        const auto expr = akkaradb::query::Column<&PlannerRecord::age>{}.in({1U, 2U, 3U})
            || akkaradb::query::Column<&PlannerRecord::age>{}.in({3U, 4U});
        const auto rows = table.query(expr).toVector();
        AKK_TEST_CHECK(rows.size() == 4);
    }
}

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();

    testAndRangeIntersection();
    testAndContradiction();
    testAndInRangeIntersection();
    testOrRangeMerge();
    testOrInDedupe();
    std::printf("AkkaraDB query planner smoke test passed\n");
    return 0;
}

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// benchmarks/smoke/typed_api_smoke_test.cpp
#include "TestErrorHandlers.hpp"

#include "akkaradb/AkkaraDB.hpp"

#include <cassert>
#include <cstdint>
#include <filesystem>
#include <format>
#include <map>
#include <optional>
#include <string>
#include <vector>
#include <chrono>

namespace {
    namespace fs = std::filesystem;

    struct TempDir {
        fs::path path;

        explicit TempDir(const char* name)
            : path{fs::temp_directory_path() / std::format("akkara_{}_{}", name, std::chrono::steady_clock::now().time_since_epoch().count())} {
            fs::remove_all(path);
            fs::create_directories(path);
        }

        ~TempDir() { fs::remove_all(path); }
    };

    struct TrivialUser {
        uint64_t id;
        int64_t score;
        uint32_t age;
    };

    struct Profile {
        uint64_t id;
        std::string email;
        std::string name;
        uint32_t age;
    };

    AKKARADB_QUERYABLE(Profile, id, email, name, age)

    struct Metric {
        uint64_t id;
        int64_t score;
        double ratio;
        std::string name;
    };

    AKKARADB_QUERYABLE(Metric, id, score, ratio, name)

    struct OptionalUser {
        uint64_t id;
        std::string name;
        std::optional<std::string> nickname;
        std::optional<uint32_t> age;
    };

    AKKARADB_QUERYABLE(OptionalUser, id, name, nickname, age)

    struct Address {
        std::string city;
        uint32_t postal_code;
    };

    struct NestedUser {
        uint64_t id;
        std::string name;
        Address address;
        uint32_t age;
    };

    AKKARADB_QUERYABLE(NestedUser, id, name, address, age)

    struct MapUser {
        uint64_t id;
        std::string name;
        std::map<std::string, std::string> tags;
        std::map<std::string, uint32_t> scores;
    };

    AKKARADB_QUERYABLE(MapUser, id, name, tags, scores)

    void test_trivial_crud() {
        using namespace akkaradb;
        TempDir dir{"trivial_crud"};
        auto db = AkkaraDB::open(dir.path, StartupMode::ULTRA_FAST);
        auto users = db->table<&TrivialUser::id>("trivial_users");

        users.put({1, 10, 20});
        assert(users.exists(1));

        auto got = users.get(1);
        assert(got.has_value());
        assert(got->score == 10);
        assert(got->age == 20);

        TrivialUser out{};
        assert(users.get_into(1, out));
        assert(out.id == 1);
        assert(out.score == 10);

        users.remove(1);
        assert(!users.exists(1));
        assert(!users.get(1).has_value());
    }

    void test_binpack_roundtrip() {
        using namespace akkaradb;
        TempDir dir{"binpack_roundtrip"};
        auto db = AkkaraDB::open(dir.path, StartupMode::ULTRA_FAST);
        auto profiles = db->table<&Profile::id>("profiles");

        profiles.put({7, "alice@example.test", "Alice", 31});
        auto got = profiles.get(7);
        assert(got.has_value());
        assert(got->email == "alice@example.test");
        assert(got->name == "Alice");
        assert(got->age == 31);
    }

    void test_non_unique_index_and_cleanup() {
        using namespace akkaradb;
        TempDir dir{"non_unique_index"};
        auto db = AkkaraDB::open(dir.path, StartupMode::ULTRA_FAST);
        auto profiles = db->table<&Profile::id>("indexed_profiles");
        auto by_age = profiles.index<&Profile::age>();
        auto by_email = profiles.index<&Profile::email>();

        profiles.put({1, "a@example.test", "Alice", 30});
        profiles.put({2, "b@example.test", "Bob", 30});
        profiles.put({3, "c@example.test", "Carol", 40});

        size_t age30 = 0;
        auto age30_range = by_age.find(30);
        while (age30_range.has_next()) {
            auto entry = age30_range.next();
            assert(entry.value.age == 30);
            ++age30;
        }
        assert(age30 == 2);

        profiles.put({2, "b2@example.test", "Bob", 41});

        size_t old_age30 = 0;
        auto old_age_range = by_age.find(30);
        while (old_age_range.has_next()) {
            auto entry = old_age_range.next();
            assert(entry.id != 2);
            ++old_age30;
        }
        assert(old_age30 == 1);

        size_t new_email = 0;
        auto email_range = by_email.find(std::string{"b2@example.test"});
        while (email_range.has_next()) {
            auto entry = email_range.next();
            assert(entry.id == 2);
            ++new_email;
        }
        assert(new_email == 1);

        profiles.remove(1);
        size_t after_remove = 0;
        auto after_remove_range = by_age.find(30);
        while (after_remove_range.has_next()) {
            (void)after_remove_range.next();
            ++after_remove;
        }
        assert(after_remove == 0);
    }

    void test_count_and_scan_are_table_scoped() {
        using namespace akkaradb;
        TempDir dir{"count_scan_scope"};
        auto db = AkkaraDB::open(dir.path, StartupMode::ULTRA_FAST);
        auto a = db->table<&TrivialUser::id>("table_a");
        auto b = db->table<&TrivialUser::id>("table_b");
        auto age = a.index<&TrivialUser::age>();

        a.put({1, 10, 20});
        a.put({2, 20, 20});
        b.put({1, 99, 99});

        assert(a.count() == 2);
        assert(b.count() == 1);

        size_t scanned = 0;
        auto scan = a.scan_all();
        while (scan.has_next()) {
            auto entry = scan.next();
            assert(entry.value.age == 20);
            ++scanned;
        }
        assert(scanned == 2);

        size_t indexed = 0;
        auto range = age.find(20);
        while (range.has_next()) {
            (void)range.next();
            ++indexed;
        }
        assert(indexed == 2);
    }

    void test_specv4_query_and_helpers() {
        using namespace akkaradb;
        TempDir dir{"specv4_query"};
        auto db = AkkaraDB::open(dir.path, StartupMode::ULTRA_FAST);
        auto profiles = db->table<&Profile::id>("query_profiles");
        profiles.indexed<&Profile::email>().indexed<&Profile::age>().indexed<&Profile::name>();

        profiles.put({1, "a@example.test", "Alice", 17});
        profiles.put({2, "b@example.test", "Bob", 30});
        profiles.put({3, "c@example.test", "Carol", 41});

        profiles.upsert(2, [](Profile& profile) {
            profile.name = "Bobby";
            profile.age = 31;
        });
        auto bob = profiles.get(2);
        assert(bob.has_value());
        assert(bob->name == "Bobby");
        assert(bob->age == 31);

        auto by_email = profiles.find_by<&Profile::email>(std::string{"b@example.test"});
        assert(by_email.has_value());
        assert(by_email->id == 2);

        auto adults = profiles.query([](auto profile) { return profile.age >= 18; }).limit(2).to_vector();
        assert(adults.size() == 2);

        auto senior = profiles.query()
            .where([](auto profile) { return profile.age >= 18; })
            .where([](auto profile) { return profile.name == "Carol"; })
            .first();
        assert(senior.has_value());
        assert(senior->id == 3);
        assert(profiles.query([](auto profile) { return profile.age > 40; }).any());
        assert(profiles.query([](auto profile) { return profile.age >= 18; }).count() == 2);

        auto indexed_email = profiles.query([](auto profile) { return profile.email == "b@example.test"; }).first();
        assert(indexed_email.has_value());
        assert(indexed_email->id == 2);

        auto indexed_residual = profiles.query([](auto profile) {
            return profile.email == "b@example.test" && profile.age >= 18;
        }).first();
        assert(indexed_residual.has_value());
        assert(indexed_residual->id == 2);

        auto fallback_or = profiles.query([](auto profile) {
            return profile.name == "Alice" || profile.age > 40;
        }).to_vector();
        assert(fallback_or.size() == 2);

        auto selected_names = profiles.query([](auto profile) {
            return profile.name.in({"Alice", "Carol"});
        }).to_vector();
        assert(selected_names.size() == 2);

        auto selected_emails = profiles.query([](auto profile) {
            return profile.email.in({"a@example.test", "c@example.test"});
        }).to_vector();
        assert(selected_emails.size() == 2);

        auto selected_indexed_ages = profiles.query([](auto profile) {
            return profile.age.in({17U, 41U});
        }).to_vector();
        assert(selected_indexed_ages.size() == 2);

        auto prefixed_email = profiles.query([](auto profile) {
            return profile.email.starts_with("b@");
        }).to_vector();
        assert(prefixed_email.size() == 1);
        assert(prefixed_email[0].value.id == 2);

        auto exact_like_email = profiles.query([](auto profile) {
            return profile.email.like("b@example.test");
        }).to_vector();
        assert(exact_like_email.size() == 1);
        assert(exact_like_email[0].value.id == 2);

        auto liked_email = profiles.query([](auto profile) {
            return profile.email.like("c@%");
        }).to_vector();
        assert(liked_email.size() == 1);
        assert(liked_email[0].value.id == 3);

        auto contained_email = profiles.query([](auto profile) {
            return profile.email.contains("b@");
        }).to_vector();
        assert(contained_email.size() == 1);
        assert(contained_email[0].value.id == 2);

        auto excluded_ages = profiles.query([](auto profile) {
            return profile.age.not_in(std::vector<uint32_t>{30, 41});
        }).to_vector();
        assert(excluded_ages.size() == 1);
        assert(excluded_ages[0].value.id == 2);

        size_t ranged = 0;
        auto range = profiles.scan(2ULL, 4ULL);
        while (range.has_next()) {
            auto entry = range.next();
            assert(entry.id == 2 || entry.id == 3);
            ++ranged;
        }
        assert(ranged == 2);
    }

    void test_non_unsigned_range_query_uses_index_source() {
        using namespace akkaradb;
        TempDir dir{"non_unsigned_range_query"};
        auto db = AkkaraDB::open(dir.path, StartupMode::ULTRA_FAST);
        auto metrics = db->table<&Metric::id>("query_metrics");
        metrics.indexed<&Metric::score>().indexed<&Metric::ratio>().indexed<&Metric::name>();

        metrics.put({1, -10, 0.25, "alpha"});
        metrics.put({2, 0, 1.5, "beta"});
        metrics.put({3, 25, 3.0, "gamma"});

        auto negative = metrics.query([](auto metric) {
            return metric.score < 0;
        }).to_vector();
        assert(negative.size() == 1);
        assert(negative[0].value.id == 1);

        auto positive = metrics.query([](auto metric) {
            return metric.score >= 0;
        }).to_vector();
        assert(positive.size() == 2);

        auto high_ratio = metrics.query([](auto metric) {
            return metric.ratio > 1.0;
        }).to_vector();
        assert(high_ratio.size() == 2);

        auto after_alpha = metrics.query([](auto metric) {
            return metric.name > "alpha";
        }).to_vector();
        assert(after_alpha.size() == 2);
    }

    void test_optional_null_query_helpers() {
        using namespace akkaradb;
        TempDir dir{"optional_null_query"};
        auto db = AkkaraDB::open(dir.path, StartupMode::ULTRA_FAST);
        auto users = db->table<&OptionalUser::id>("optional_users");
        users.indexed<&OptionalUser::nickname>().indexed<&OptionalUser::age>();

        users.put({1, "Alice", std::nullopt, 30U});
        users.put({2, "Bob", std::string{"Bobby"}, std::nullopt});
        users.put({3, "Carol", std::nullopt, std::nullopt});

        auto missing_nicknames = users.query([](auto user) {
            return user.nickname.is_null();
        }).to_vector();
        assert(missing_nicknames.size() == 2);

        auto present_nicknames = users.query([](auto user) {
            return user.nickname.is_not_null();
        }).to_vector();
        assert(present_nicknames.size() == 1);
        assert(present_nicknames[0].value.id == 2);

        auto named_adults = users.query([](auto user) {
            return user.nickname.is_not_null() || user.age.is_not_null();
        }).to_vector();
        assert(named_adults.size() == 2);
    }

    void test_nested_field_query_helpers() {
        using namespace akkaradb;
        TempDir dir{"nested_field_query"};
        auto db = AkkaraDB::open(dir.path, StartupMode::ULTRA_FAST);
        auto users = db->table<&NestedUser::id>("nested_users");

        users.put({1, "Alice", {"Tokyo", 100}, 30});
        users.put({2, "Bob", {"Osaka", 530}, 41});
        users.put({3, "Carol", {"Tokyo", 150}, 22});

        auto tokyo_users = users.query([](auto user) {
            return user.address.template field<&Address::city>() == "Tokyo";
        }).to_vector();
        assert(tokyo_users.size() == 2);

        auto central_tokyo = users.query([](auto user) {
            return user.address.template field<&Address::city>() == "Tokyo" && user.address.template field<&Address::postal_code>() < 120U;
        }).to_vector();
        assert(central_tokyo.size() == 1);
        assert(central_tokyo[0].value.id == 1);
    }

    void test_map_get_query_helpers() {
        using namespace akkaradb;
        TempDir dir{"map_get_query"};
        auto db = AkkaraDB::open(dir.path, StartupMode::ULTRA_FAST);
        auto users = db->table<&MapUser::id>("map_users");

        users.put({1, "Alice", {{"tier", "gold"}, {"region", "jp"}}, {{"score", 90}}});
        users.put({2, "Bob", {{"tier", "silver"}}, {{"score", 70}}});
        users.put({3, "Carol", {{"region", "us"}}, {{"score", 85}}});

        auto gold_users = users.query([](auto user) {
            return user.tags.map_get("tier") == std::optional<std::string>{"gold"};
        }).to_vector();
        assert(gold_users.size() == 1);
        assert(gold_users[0].value.id == 1);

        auto missing_tier = users.query([](auto user) {
            return user.tags.get("tier").is_null();
        }).to_vector();
        assert(missing_tier.size() == 1);
        assert(missing_tier[0].value.id == 3);

        auto high_scores = users.query([](auto user) {
            return user.scores.map_get("score") >= std::optional<uint32_t>{85U};
        }).to_vector();
        assert(high_scores.size() == 2);
    }
} // namespace

int main() {
    akkara::test::install_msvc_test_error_handlers();

    test_trivial_crud();
    test_binpack_roundtrip();
    test_non_unique_index_and_cleanup();
    test_count_and_scan_are_table_scoped();
    test_specv4_query_and_helpers();
    test_non_unsigned_range_query_uses_index_source();
    test_optional_null_query_helpers();
    test_nested_field_query_helpers();
    test_map_get_query_helpers();
    return 0;
}

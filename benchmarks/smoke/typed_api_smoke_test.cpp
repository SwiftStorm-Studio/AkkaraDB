/*
 * AkkaraDB - Typed API smoke test.
 */

#include "akkaradb/AkkaraDB.hpp"

#include <cassert>
#include <cstdint>
#include <filesystem>
#include <format>
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

    void test_trivial_crud() {
        using namespace akkaradb;
        auto db = AkkaraDB::open({}, StartupMode::ULTRA_FAST);
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
        auto db = AkkaraDB::open({}, StartupMode::ULTRA_FAST);
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
        auto db = AkkaraDB::open({}, StartupMode::ULTRA_FAST);
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
        auto db = AkkaraDB::open({}, StartupMode::ULTRA_FAST);
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
        auto db = AkkaraDB::open({}, StartupMode::ULTRA_FAST);
        auto profiles = db->table<&Profile::id>("query_profiles");
        profiles.indexed<&Profile::email>().indexed<&Profile::age>();

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

        auto prefixed_email = profiles.query([](auto profile) {
            return profile.email.starts_with("b@");
        }).to_vector();
        assert(prefixed_email.size() == 1);
        assert(prefixed_email[0].value.id == 2);

        auto liked_email = profiles.query([](auto profile) {
            return profile.email.like("c@%");
        }).to_vector();
        assert(liked_email.size() == 1);
        assert(liked_email[0].value.id == 3);

        auto contained_name = profiles.query([](auto profile) {
            return profile.name.contains("lic");
        }).to_vector();
        assert(contained_name.size() == 1);
        assert(contained_name[0].value.id == 1);

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
} // namespace

int main() {
    test_trivial_crud();
    test_binpack_roundtrip();
    test_non_unique_index_and_cleanup();
    test_count_and_scan_are_table_scoped();
    test_specv4_query_and_helpers();
    return 0;
}

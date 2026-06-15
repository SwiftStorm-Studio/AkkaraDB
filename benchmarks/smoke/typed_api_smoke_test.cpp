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

// benchmarks/smoke/typedApiSmokeTest.cpp
#include "TestErrorHandlers.hpp"

#include "akkaradb/AkkaraDB.hpp"

#include <cstdint>
#include <filesystem>
#include <format>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>
#include <chrono>

struct RefAuthor {
    uint64_t id;
    std::string name;
    uint32_t age;
    std::string email;
};

AKKARADB_ENTITY(RefAuthor, id, name, age, email);

struct RefPost {
    uint64_t id;
    akkaradb::Ref<RefAuthor> author;
    std::string body;
    uint32_t likes;
    std::string title;
};

AKKARADB_ENTITY(RefPost, id, author, body, likes);

struct PlainPost {
    uint64_t id;
    uint64_t authorId;
    uint32_t authorAge;
    std::string body;
    uint32_t likes;
};

AKKARADB_QUERYABLE(PlainPost, id, authorId, authorAge, body);

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
        uint32_t postalCode;
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

    void testTrivialCrud() {
        using namespace akkaradb;
        TempDir dir{"trivialCrud"};
        auto db = AkkaraDB::open(dir.path, StartupMode::ULTRA_FAST);
        auto users = db->table<&TrivialUser::id>("trivialUsers");

        users.put({1, 10, 20});
        AKK_TEST_CHECK(users.exists(1));

        auto got = users.get(1);
        AKK_TEST_CHECK(got.has_value());
        AKK_TEST_CHECK(got->score == 10);
        AKK_TEST_CHECK(got->age == 20);

        TrivialUser out{};
        AKK_TEST_CHECK(users.getInto(1, out));
        AKK_TEST_CHECK(out.id == 1);
        AKK_TEST_CHECK(out.score == 10);

        users.remove(1);
        AKK_TEST_CHECK(!users.exists(1));
        AKK_TEST_CHECK(!users.get(1).has_value());
    }

    void testBinpackRoundtrip() {
        using namespace akkaradb;
        TempDir dir{"binpackRoundtrip"};
        auto db = AkkaraDB::open(dir.path, StartupMode::ULTRA_FAST);
        auto profiles = db->table<&Profile::id>("profiles");

        profiles.put({7, "alice@example.test", "Alice", 31});
        auto got = profiles.get(7);
        AKK_TEST_CHECK(got.has_value());
        AKK_TEST_CHECK(got->email == "alice@example.test");
        AKK_TEST_CHECK(got->name == "Alice");
        AKK_TEST_CHECK(got->age == 31);
    }

    void testNonUniqueIndexAndCleanup() {
        using namespace akkaradb;
        TempDir dir{"nonUniqueIndex"};
        auto db = AkkaraDB::open(dir.path, StartupMode::ULTRA_FAST);
        auto profiles = db->table<&Profile::id>("indexedProfiles");
        auto byAge = profiles.index<&Profile::age>();
        auto byEmail = profiles.index<&Profile::email>();

        profiles.put({1, "a@example.test", "Alice", 30});
        profiles.put({2, "b@example.test", "Bob", 30});
        profiles.put({3, "c@example.test", "Carol", 40});

        size_t age30 = 0;
        auto age30Range = byAge.find(30);
        while (age30Range.hasNext()) {
            auto entry = age30Range.next();
            AKK_TEST_CHECK(entry.value.age == 30);
            ++age30;
        }
        AKK_TEST_CHECK(age30 == 2);

        profiles.put({2, "b2@example.test", "Bob", 41});

        size_t oldAge30 = 0;
        auto oldAgeRange = byAge.find(30);
        while (oldAgeRange.hasNext()) {
            auto entry = oldAgeRange.next();
            AKK_TEST_CHECK(entry.id != 2);
            ++oldAge30;
        }
        AKK_TEST_CHECK(oldAge30 == 1);

        size_t newEmail = 0;
        auto emailRange = byEmail.find(std::string{"b2@example.test"});
        while (emailRange.hasNext()) {
            auto entry = emailRange.next();
            AKK_TEST_CHECK(entry.id == 2);
            ++newEmail;
        }
        AKK_TEST_CHECK(newEmail == 1);

        profiles.remove(1);
        size_t afterRemove = 0;
        auto afterRemoveRange = byAge.find(30);
        while (afterRemoveRange.hasNext()) {
            (void)afterRemoveRange.next();
            ++afterRemove;
        }
        AKK_TEST_CHECK(afterRemove == 0);
    }

    void testCountAndScanAreTableScoped() {
        using namespace akkaradb;
        TempDir dir{"countScanScope"};
        auto db = AkkaraDB::open(dir.path, StartupMode::ULTRA_FAST);
        auto a = db->table<&TrivialUser::id>("tableA");
        auto b = db->table<&TrivialUser::id>("tableB");
        auto age = a.index<&TrivialUser::age>();

        a.put({1, 10, 20});
        a.put({2, 20, 20});
        b.put({1, 99, 99});

        AKK_TEST_CHECK(a.count() == 2);
        AKK_TEST_CHECK(b.count() == 1);

        size_t scanned = 0;
        auto scan = a.scanAll();
        while (scan.hasNext()) {
            auto entry = scan.next();
            AKK_TEST_CHECK(entry.value.age == 20);
            ++scanned;
        }
        AKK_TEST_CHECK(scanned == 2);

        size_t indexed = 0;
        auto range = age.find(20);
        while (range.hasNext()) {
            (void)range.next();
            ++indexed;
        }
        AKK_TEST_CHECK(indexed == 2);
    }

    void testSpecv4QueryAndHelpers() {
        using namespace akkaradb;
        TempDir dir{"specv4Query"};
        auto db = AkkaraDB::open(dir.path, StartupMode::ULTRA_FAST);
        auto profiles = db->table<&Profile::id>("queryProfiles");
        profiles.indexed<&Profile::email>().indexed<&Profile::age>().indexed<&Profile::name>();

        profiles.put({1, "a@example.test", "Alice", 17});
        profiles.put({2, "b@example.test", "Bob", 30});
        profiles.put({3, "c@example.test", "Carol", 41});

        profiles.upsert(2, [](Profile& profile) {
            profile.name = "Bobby";
            profile.age = 31;
        });
        auto bob = profiles.get(2);
        AKK_TEST_CHECK(bob.has_value());
        AKK_TEST_CHECK(bob->name == "Bobby");
        AKK_TEST_CHECK(bob->age == 31);

        auto byEmail = profiles.findBy<&Profile::email>(std::string{"b@example.test"});
        AKK_TEST_CHECK(byEmail.has_value());
        AKK_TEST_CHECK(byEmail->id == 2);

        auto adults = profiles.query([](auto profile) { return profile.age >= 18; }).limit(2).toVector();
        AKK_TEST_CHECK(adults.size() == 2);

        auto senior = profiles.query()
            .where([](auto profile) { return profile.age >= 18; })
            .where([](auto profile) { return profile.name == "Carol"; })
            .first();
        AKK_TEST_CHECK(senior.has_value());
        AKK_TEST_CHECK(senior->id == 3);
        AKK_TEST_CHECK(profiles.query([](auto profile) { return profile.age > 40; }).any());
        AKK_TEST_CHECK(profiles.query([](auto profile) { return profile.age >= 18; }).count() == 2);

        auto indexedEmail = profiles.query([](auto profile) { return profile.email == "b@example.test"; }).first();
        AKK_TEST_CHECK(indexedEmail.has_value());
        AKK_TEST_CHECK(indexedEmail->id == 2);

        auto indexedResidual = profiles.query([](auto profile) {
            return profile.email == "b@example.test" && profile.age >= 18;
        }).first();
        AKK_TEST_CHECK(indexedResidual.has_value());
        AKK_TEST_CHECK(indexedResidual->id == 2);

        auto fallbackOr = profiles.query([](auto profile) {
            return profile.name == "Alice" || profile.age > 40;
        }).toVector();
        AKK_TEST_CHECK(fallbackOr.size() == 2);

        auto selectedNames = profiles.query([](auto profile) {
            return profile.name.in({"Alice", "Carol"});
        }).toVector();
        AKK_TEST_CHECK(selectedNames.size() == 2);

        auto selectedEmails = profiles.query([](auto profile) {
            return profile.email.in({"a@example.test", "c@example.test"});
        }).toVector();
        AKK_TEST_CHECK(selectedEmails.size() == 2);

        auto selectedIndexedAges = profiles.query([](auto profile) {
            return profile.age.in({17U, 41U});
        }).toVector();
        AKK_TEST_CHECK(selectedIndexedAges.size() == 2);

        auto prefixedEmail = profiles.query([](auto profile) {
            return profile.email.startsWith("b@");
        }).toVector();
        AKK_TEST_CHECK(prefixedEmail.size() == 1);
        AKK_TEST_CHECK(prefixedEmail[0].value.id == 2);

        auto exactLikeEmail = profiles.query([](auto profile) {
            return profile.email.like("b@example.test");
        }).toVector();
        AKK_TEST_CHECK(exactLikeEmail.size() == 1);
        AKK_TEST_CHECK(exactLikeEmail[0].value.id == 2);

        auto likedEmail = profiles.query([](auto profile) {
            return profile.email.like("c@%");
        }).toVector();
        AKK_TEST_CHECK(likedEmail.size() == 1);
        AKK_TEST_CHECK(likedEmail[0].value.id == 3);

        auto containedEmail = profiles.query([](auto profile) {
            return profile.email.contains("b@");
        }).toVector();
        AKK_TEST_CHECK(containedEmail.size() == 1);
        AKK_TEST_CHECK(containedEmail[0].value.id == 2);

        auto excludedAges = profiles.query([](auto profile) {
            return profile.age.notIn(std::vector<uint32_t>{17, 41});
        }).toVector();
        AKK_TEST_CHECK(excludedAges.size() == 1);
        AKK_TEST_CHECK(excludedAges[0].value.id == 2);

        size_t ranged = 0;
        auto range = profiles.scan(2ULL, 4ULL);
        while (range.hasNext()) {
            auto entry = range.next();
            AKK_TEST_CHECK(entry.id == 2 || entry.id == 3);
            ++ranged;
        }
        AKK_TEST_CHECK(ranged == 2);
    }

    void testNonUnsignedRangeQueryUsesIndexSource() {
        using namespace akkaradb;
        TempDir dir{"nonUnsignedRangeQuery"};
        auto db = AkkaraDB::open(dir.path, StartupMode::ULTRA_FAST);
        auto metrics = db->table<&Metric::id>("queryMetrics");
        metrics.indexed<&Metric::score>().indexed<&Metric::ratio>().indexed<&Metric::name>();

        metrics.put({1, -10, 0.25, "alpha"});
        metrics.put({2, 0, 1.5, "beta"});
        metrics.put({3, 25, 3.0, "gamma"});

        const auto betaMetric = metrics.get(2);
        AKK_TEST_CHECK(betaMetric.has_value());
        AKK_TEST_CHECK(betaMetric->name == "beta");

        auto negative = metrics.query([](auto metric) {
            return metric.score < 0;
        }).toVector();
        AKK_TEST_CHECK(negative.size() == 1);
        AKK_TEST_CHECK(negative[0].value.id == 1);

        auto positive = metrics.query([](auto metric) {
            return metric.score >= 0;
        }).toVector();
        AKK_TEST_CHECK(positive.size() == 2);

        auto highRatio = metrics.query([](auto metric) {
            return metric.ratio > 1.0;
        }).toVector();
        AKK_TEST_CHECK(highRatio.size() == 2);

        auto afterAlpha = metrics.query([](auto metric) {
            return metric.name > "alpha";
        }).toVector();
        AKK_TEST_CHECK(afterAlpha.size() == 2);
    }

    void testOptionalNullQueryHelpers() {
        using namespace akkaradb;
        TempDir dir{"optionalNullQuery"};
        auto db = AkkaraDB::open(dir.path, StartupMode::ULTRA_FAST);
        auto users = db->table<&OptionalUser::id>("optionalUsers");
        users.indexed<&OptionalUser::nickname>().indexed<&OptionalUser::age>();

        users.put({1, "Alice", std::nullopt, 30U});
        users.put({2, "Bob", std::string{"Bobby"}, std::nullopt});
        users.put({3, "Carol", std::nullopt, std::nullopt});

        auto missingNicknames = users.query([](auto user) {
            return user.nickname.isNull();
        }).toVector();
        AKK_TEST_CHECK(missingNicknames.size() == 2);

        auto presentNicknames = users.query([](auto user) {
            return user.nickname.isNotNull();
        }).toVector();
        AKK_TEST_CHECK(presentNicknames.size() == 1);
        AKK_TEST_CHECK(presentNicknames[0].value.id == 2);

        auto namedAdults = users.query([](auto user) {
            return user.nickname.isNotNull() || user.age.isNotNull();
        }).toVector();
        AKK_TEST_CHECK(namedAdults.size() == 2);
    }

    void testNestedFieldQueryHelpers() {
        using namespace akkaradb;
        TempDir dir{"nestedFieldQuery"};
        auto db = AkkaraDB::open(dir.path, StartupMode::ULTRA_FAST);
        auto users = db->table<&NestedUser::id>("nestedUsers");

        users.put({1, "Alice", {"Tokyo", 100}, 30});
        users.put({2, "Bob", {"Osaka", 530}, 41});
        users.put({3, "Carol", {"Tokyo", 150}, 22});

        auto tokyoUsers = users.query([](auto user) {
            return user.address.template field<&Address::city>() == "Tokyo";
        }).toVector();
        AKK_TEST_CHECK(tokyoUsers.size() == 2);

        auto centralTokyo = users.query([](auto user) {
            return user.address.template field<&Address::city>() == "Tokyo" && user.address.template field<&Address::postalCode>() < 120U;
        }).toVector();
        AKK_TEST_CHECK(centralTokyo.size() == 1);
        AKK_TEST_CHECK(centralTokyo[0].value.id == 1);
    }

    void testMapGetQueryHelpers() {
        using namespace akkaradb;
        TempDir dir{"mapGetQuery"};
        auto db = AkkaraDB::open(dir.path, StartupMode::ULTRA_FAST);
        auto users = db->table<&MapUser::id>("mapUsers");

        users.put({1, "Alice", {{"tier", "gold"}, {"region", "jp"}}, {{"score", 90}}});
        users.put({2, "Bob", {{"tier", "silver"}}, {{"score", 70}}});
        users.put({3, "Carol", {{"region", "us"}}, {{"score", 85}}});

        auto goldUsers = users.query([](auto user) {
            return user.tags.mapGet("tier") == std::optional<std::string>{"gold"};
        }).toVector();
        AKK_TEST_CHECK(goldUsers.size() == 1);
        AKK_TEST_CHECK(goldUsers[0].value.id == 1);

        auto missingTier = users.query([](auto user) {
            return user.tags.get("tier").isNull();
        }).toVector();
        AKK_TEST_CHECK(missingTier.size() == 1);
        AKK_TEST_CHECK(missingTier[0].value.id == 3);

        auto highScores = users.query([](auto user) {
            return user.scores.mapGet("score") >= std::optional<uint32_t>{85U};
        }).toVector();
        AKK_TEST_CHECK(highScores.size() == 2);
    }

    void testRefLazyResolveAndCascadePut() {
        using namespace akkaradb;
        TempDir dir{"refLazyResolve"};
        auto db = AkkaraDB::open(dir.path, StartupMode::ULTRA_FAST);
        auto schema = db->schema()
            .table<&RefAuthor::id>("authors")
            .table<&RefPost::id>("posts")
            .foreignKey<&RefPost::author>()
            .open();
        auto& authors = schema.table<RefAuthor>();
        auto& posts = schema.table<RefPost>();

        authors.put({1, "Alice", 30, "alice@example.test"});
        posts.put({100, ref<RefAuthor>(1), "hello", 5, "first"});

        auto post = posts.get(100);
        AKK_TEST_CHECK(post.has_value());
        AKK_TEST_CHECK(post->author.id() == 1);
        AKK_TEST_CHECK(post->author->name == "Alice");

        auto joined = posts.join<&RefPost::author>(authors).toVector();
        AKK_TEST_CHECK(joined.size() == 1);
        AKK_TEST_CHECK(joined[0].left.value.id == 100);
        AKK_TEST_CHECK(joined[0].right.name == "Alice");

        auto joinedAlice = posts
            .join<&RefPost::author>(authors)
            .where([](const RefPost& joinedPost, const RefAuthor& author) {
                return joinedPost.likes == 5 && author.name == "Alice";
            })
            .first();
        AKK_TEST_CHECK(joinedAlice.has_value());
        AKK_TEST_CHECK(joinedAlice->left.id == 100);

        auto plainPosts = db->table<&PlainPost::id>("plain_posts");
        plainPosts.put({200, 1, 30, "plain pk join", 7});
        plainPosts.put({201, 404, 30, "plain field join", 2});

        auto plainPkJoined = plainPosts.join<&PlainPost::authorId, &RefAuthor::id>(authors).toVector();
        AKK_TEST_CHECK(plainPkJoined.size() == 1);
        AKK_TEST_CHECK(plainPkJoined[0].left.value.id == 200);
        AKK_TEST_CHECK(plainPkJoined[0].right.name == "Alice");

        auto plainFieldJoined = plainPosts.join<&PlainPost::authorAge, &RefAuthor::age>(authors).toVector();
        AKK_TEST_CHECK(plainFieldJoined.size() == 2);
        AKK_TEST_CHECK(plainFieldJoined[0].right.name == "Alice");

        post->author->name = "Alice Updated";
        posts.put(*post);

        auto updatedAuthor = authors.get(1);
        AKK_TEST_CHECK(updatedAuthor.has_value());
        AKK_TEST_CHECK(updatedAuthor->name == "Alice Updated");

        posts.put({101, RefAuthor{2, "Bob", 40, "bob@example.test"}, "from value", 3, "second"});
        auto bob = authors.get(2);
        AKK_TEST_CHECK(bob.has_value());
        AKK_TEST_CHECK(bob->name == "Bob");

        authors.remove(2);
        AKK_TEST_CHECK(!authors.exists(2));
        AKK_TEST_CHECK(!posts.exists(101));

        bool missingAuthorRejected = false;
        try {
            posts.put({102, ref<RefAuthor>(404), "missing", 1, "broken"});
        }
        catch (const std::runtime_error&) {
            missingAuthorRejected = true;
        }
        AKK_TEST_CHECK(missingAuthorRejected);
        AKK_TEST_CHECK(!posts.exists(102));
    }
} // namespace

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();

    testTrivialCrud();
    testBinpackRoundtrip();
    testNonUniqueIndexAndCleanup();
    testCountAndScanAreTableScoped();
    testSpecv4QueryAndHelpers();
    testNonUnsignedRangeQueryUsesIndexSource();
    testOptionalNullQueryHelpers();
    testNestedFieldQueryHelpers();
    testMapGetQueryHelpers();
    testRefLazyResolveAndCascadePut();
    return 0;
}

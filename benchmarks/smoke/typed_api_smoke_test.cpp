/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// benchmarks/smoke/typed_api_smoke_test.cpp
#include "TestErrorHandlers.hpp"

#include "akkaradb/AkkaraDB.hpp"

#include <array>
#include <cstdint>
#include <filesystem>
#include <format>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <sstream>
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

AKKARADB_ENTITY(PlainPost, id, authorId, authorAge, body);

struct RestrictedPost {
    uint64_t id;
    uint64_t authorId;
    std::string body;
    uint32_t likes;
};

AKKARADB_ENTITY(RestrictedPost, id, authorId, body, likes);

struct NullablePost {
    uint64_t id;
    std::optional<uint64_t> authorId;
    std::string body;
    uint32_t likes;
};

AKKARADB_ENTITY(NullablePost, id, authorId, body, likes);

struct ImmutableUser {
    uint64_t id;
    akkaradb::Immutable<std::string> handle;
    uint32_t age;
    std::string bio;
};

AKKARADB_ENTITY(ImmutableUser, id, handle, age, bio);

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

    struct SignedKeyUser {
        int64_t id;
        std::string name;
        int64_t score;
        uint32_t age;
    };

    AKKARADB_QUERYABLE(SignedKeyUser, id, name, score, age)

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

    struct WideQueryable {
        uint64_t id;
        uint32_t f01;
        uint32_t f02;
        uint32_t f03;
        uint32_t f04;
        uint32_t f05;
        uint32_t f06;
        uint32_t f07;
        uint32_t f08;
        uint32_t f09;
        uint32_t f10;
        std::string label;
    };

    AKKARADB_QUERYABLE(WideQueryable, id, f01, f02, f03, f04, f05, f06, f07, f08, f09, f10, label)

    bool profileIsBobbyWithOddAge(const Profile& profile, const void*) {
        return profile.name == "Bobby" && (profile.age % 2U) == 1U;
    }

    struct ProfileMinAgeCapture {
        uint32_t minAge;
    };

    struct ProfileNameCapture {
        std::string name;
    };

    bool profileAgeAtLeastCapture(const Profile& profile, const void* raw) {
        const auto* capture = static_cast<const ProfileMinAgeCapture*>(raw);
        return capture != nullptr && profile.age >= capture->minAge;
    }

    bool profileNameEqualsCapture(const Profile& profile, const void* raw) {
        const auto* capture = static_cast<const ProfileNameCapture*>(raw);
        return capture != nullptr && profile.name == capture->name;
    }

    bool oddAgeOpcode(
        std::span<const akkaradb::query::bytecode::Value> args,
        akkaradb::query::bytecode::Value& out,
        const void*
    ) {
        namespace bc = akkaradb::query::bytecode;
        if (args.size() != 1 || !args[0].isNumeric()) { return false; }
        const auto value = args[0].kind == bc::ValueKind::UInt
                               ? args[0].u
                               : static_cast<uint64_t>(bc::numericAsLongDouble(args[0]));
        out = bc::Value::boolean((value % 2U) == 1U);
        return true;
    }

    bool rejectingOpcode(
        std::span<const akkaradb::query::bytecode::Value>,
        akkaradb::query::bytecode::Value&,
        const void*
    ) {
        return false;
    }

    bool customOddAge(uint32_t age) {
        return (age % 2U) == 1U;
    }

    AKKARADB_QUERY_OPCODE(
        customOddAge,
        akkaradb::query::bytecode::customOpcodeUserMin + 1,
        "custom_odd_age",
        1,
        akkaradb::query::bytecode::ValueKind::Bool,
        &oddAgeOpcode
    );

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

    void testPrimaryKeyRangeScanUsesNumericOrder() {
        using namespace akkaradb;
        TempDir dir{"primaryKeyRangeOrder"};
        auto db = AkkaraDB::open(dir.path, StartupMode::ULTRA_FAST);
        auto users = db->table<&TrivialUser::id>("rangeUsers");
        auto signedUsers = db->table<&SignedKeyUser::id>("signedRangeUsers");

        users.put({1, 10, 20});
        users.put({99, 20, 21});
        users.put({100, 30, 22});
        users.put({256, 40, 23});
        users.put({257, 50, 24});

        std::vector<uint64_t> unsignedIds;
        auto unsignedRange = users.scan(1ULL, 100ULL);
        while (unsignedRange.hasNext()) { unsignedIds.push_back(unsignedRange.next().id); }
        AKK_TEST_CHECK((unsignedIds == std::vector<uint64_t>{1, 99}));

        signedUsers.put({-3, "minus-three", 0, 20});
        signedUsers.put({-1, "minus-one", 0, 21});
        signedUsers.put({0, "zero", 0, 22});
        signedUsers.put({1, "one", 0, 23});
        signedUsers.put({3, "three", 0, 24});

        std::vector<int64_t> signedIds;
        auto signedRange = signedUsers.scan(-2, 2);
        while (signedRange.hasNext()) { signedIds.push_back(signedRange.next().id); }
        AKK_TEST_CHECK((signedIds == std::vector<int64_t>{-1, 0, 1}));
    }

    void testSpecv4QueryAndHelpers() {
        using namespace akkaradb;
        TempDir dir{"specv4Query"};
        auto db = AkkaraDB::open(dir.path, StartupMode::ULTRA_FAST);
        auto profiles = db->table<&Profile::id>("queryProfiles");
        profiles.indexed<&Profile::email>().indexed<&Profile::age>().indexed<&Profile::name>().prefixIndexed<&Profile::email>();

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

        auto indexedOr = profiles.query([](auto profile) {
            return profile.email == "b@example.test" || profile.name == "Carol";
        }).toVector();
        AKK_TEST_CHECK(indexedOr.size() == 2);

        auto overlappingOr = profiles.query([](auto profile) {
            return profile.age >= 18 || profile.name == "Carol";
        }).toVector();
        AKK_TEST_CHECK(overlappingOr.size() == 2);

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

        auto suffixEmails = profiles.query([](auto profile) {
            return profile.email.endsWith(".test");
        }).toVector();
        AKK_TEST_CHECK(suffixEmails.size() == 3);

        auto suffixEmailsSnake = profiles.query([](auto profile) {
            return profile.email.ends_with(".test");
        }).toVector();
        AKK_TEST_CHECK(suffixEmailsSnake.size() == 3);

        auto excludedAges = profiles.query([](auto profile) {
            return profile.age.notIn(std::vector<uint32_t>{17, 41});
        }).toVector();
        AKK_TEST_CHECK(excludedAges.size() == 1);
        AKK_TEST_CHECK(excludedAges[0].value.id == 2);

        auto prefixOnlyProfiles = db->table<&Profile::id>("prefixOnlyProfiles");
        prefixOnlyProfiles.prefixIndexed<&Profile::email>();
        prefixOnlyProfiles.put({1, "alpha@example.test", "Alpha", 20});
        prefixOnlyProfiles.put({2, "beta@example.test", "Beta", 21});
        prefixOnlyProfiles.put({3, "alphabet@example.test", "Alphabet", 22});

        auto alphaEmails = prefixOnlyProfiles.query([](auto profile) {
            return profile.email.startsWith("alpha");
        }).toVector();
        AKK_TEST_CHECK(alphaEmails.size() == 2);

        namespace bc = akkaradb::query::bytecode;
        static constexpr std::array<uint8_t, 16> bytecode{
            static_cast<uint8_t>(bc::Opcode::LoadField), 0, 0,
            static_cast<uint8_t>(bc::Opcode::PushConst), 0, 0,
            static_cast<uint8_t>(bc::Opcode::Ge),
            static_cast<uint8_t>(bc::Opcode::LoadField), 1, 0,
            static_cast<uint8_t>(bc::Opcode::PushConst), 1, 0,
            static_cast<uint8_t>(bc::Opcode::Eq),
            static_cast<uint8_t>(bc::Opcode::And),
            static_cast<uint8_t>(bc::Opcode::Return)
        };
        static constexpr std::array<bc::Value, 2> constants{
            bc::Value::uinteger(18),
            bc::Value::string("Bobby")
        };
        static constexpr std::array<bc::FieldBinding<Profile>, 2> fields{
            bc::makeTopLevelFieldBinding<&Profile::age, 3>("age"),
            bc::makeTopLevelFieldBinding<&Profile::name, 2>("name")
        };
        static constexpr std::array<bc::PlanHint, 1> planHints{
            bc::PlanHint{bc::PlanHintOp::Ge, 0, 0}
        };
        const auto encodedBobby = akkaradb::binpack::BinPack::encode(Profile{2, "b@example.test", "Bobby", 31});
        AKK_TEST_CHECK(fields[0].rawRead(encodedBobby).u == 31U);
        AKK_TEST_CHECK(fields[1].rawRead(encodedBobby).s == "Bobby");

        auto compiledTableScan = profiles.__query_compiled(
            bc::CompiledQueryDescriptor<Profile>{
                .code = bytecode,
                .constants = constants,
                .fields = fields,
                .hostCalls = {},
                .customOpcodes = {},
                .planHints = {},
                .captures = nullptr,
                .capturesOwner = {}
            }
        ).toVector();
        AKK_TEST_CHECK(compiledTableScan.size() == 1);
        AKK_TEST_CHECK(compiledTableScan[0].value.id == 2);

        auto compiled = profiles.__query_compiled(
            bc::CompiledQueryDescriptor<Profile>{
                .code = bytecode,
                .constants = constants,
                .fields = fields,
                .hostCalls = {},
                .customOpcodes = {},
                .planHints = planHints,
                .captures = nullptr,
                .capturesOwner = {}
            }
        ).toVector();
        AKK_TEST_CHECK(compiled.size() == 1);
        AKK_TEST_CHECK(compiled[0].value.id == 2);

        static constexpr std::array<uint8_t, 8> stringBytecode{
            static_cast<uint8_t>(bc::Opcode::LoadField), 0, 0,
            static_cast<uint8_t>(bc::Opcode::PushConst), 0, 0,
            static_cast<uint8_t>(bc::Opcode::EndsWith),
            static_cast<uint8_t>(bc::Opcode::Return)
        };
        static constexpr std::array<bc::Value, 1> stringConstants{
            bc::Value::string(".test")
        };
        static constexpr std::array<bc::FieldBinding<Profile>, 1> stringFields{
            bc::makeTopLevelFieldBinding<&Profile::email, 1>("email")
        };
        auto stringFiltered = profiles.__query_compiled(
            bc::CompiledQueryDescriptor<Profile>{
                .code = stringBytecode,
                .constants = stringConstants,
                .fields = stringFields,
                .hostCalls = {},
                .customOpcodes = {},
                .planHints = {},
                .captures = nullptr,
                .capturesOwner = {}
            }
        ).toVector();
        AKK_TEST_CHECK(stringFiltered.size() == 3);

        struct BytecodeCaptureOwner {
            uint32_t minAge;
            std::array<bc::Value, 1> constants;

            explicit BytecodeCaptureOwner(uint32_t value)
                : minAge{value},
                  constants{bc::valueFrom(minAge)} {}
        };
        auto captureOwner = std::make_shared<BytecodeCaptureOwner>(18U);
        static constexpr std::array<uint8_t, 8> capturedBytecode{
            static_cast<uint8_t>(bc::Opcode::LoadField), 0, 0,
            static_cast<uint8_t>(bc::Opcode::PushConst), 0, 0,
            static_cast<uint8_t>(bc::Opcode::Ge),
            static_cast<uint8_t>(bc::Opcode::Return)
        };
        auto capturedFiltered = profiles.__query_compiled(
            bc::CompiledQueryDescriptor<Profile>{
                .code = capturedBytecode,
                .constants = captureOwner->constants,
                .fields = std::span<const bc::FieldBinding<Profile>>{fields.data(), 1},
                .hostCalls = {},
                .customOpcodes = {},
                .planHints = {},
                .captures = captureOwner.get(),
                .capturesOwner = captureOwner
            }
        ).toVector();
        AKK_TEST_CHECK(capturedFiltered.size() == 2);

        static constexpr std::array<uint8_t, 8> whereNameBytecode{
            static_cast<uint8_t>(bc::Opcode::LoadField), 0, 0,
            static_cast<uint8_t>(bc::Opcode::PushConst), 0, 0,
            static_cast<uint8_t>(bc::Opcode::Eq),
            static_cast<uint8_t>(bc::Opcode::Return)
        };
        static constexpr std::array<bc::Value, 1> whereNameConstants{
            bc::Value::string("Bobby")
        };
        static constexpr std::array<bc::FieldBinding<Profile>, 1> whereNameFields{
            bc::makeTopLevelFieldBinding<&Profile::name, 2>("name")
        };
        auto composedWhereFiltered = profiles.__query_compiled(
            bc::CompiledQueryDescriptor<Profile>{
                .code = capturedBytecode,
                .constants = captureOwner->constants,
                .fields = std::span<const bc::FieldBinding<Profile>>{fields.data(), 1},
                .hostCalls = {},
                .customOpcodes = {},
                .planHints = {},
                .captures = captureOwner.get(),
                .capturesOwner = captureOwner
            }
        ).where(
            bc::CompiledQueryDescriptor<Profile>{
                .code = whereNameBytecode,
                .constants = whereNameConstants,
                .fields = whereNameFields,
                .hostCalls = {},
                .customOpcodes = {},
                .planHints = {},
                .captures = nullptr,
                .capturesOwner = {}
            }
        ).toVector();
        AKK_TEST_CHECK(composedWhereFiltered.size() == 1);
        AKK_TEST_CHECK(composedWhereFiltered[0].value.id == 2);

        static constexpr std::array<uint8_t, 12> arithmeticBytecode{
            static_cast<uint8_t>(bc::Opcode::LoadField), 0, 0,
            static_cast<uint8_t>(bc::Opcode::PushConst), 0, 0,
            static_cast<uint8_t>(bc::Opcode::Add),
            static_cast<uint8_t>(bc::Opcode::PushConst), 1, 0,
            static_cast<uint8_t>(bc::Opcode::Ge),
            static_cast<uint8_t>(bc::Opcode::Return)
        };
        static constexpr std::array<bc::Value, 2> arithmeticConstants{
            bc::Value::integer(1),
            bc::Value::integer(42)
        };
        auto arithmeticFiltered = profiles.__query_compiled(
            bc::CompiledQueryDescriptor<Profile>{
                .code = arithmeticBytecode,
                .constants = arithmeticConstants,
                .fields = std::span<const bc::FieldBinding<Profile>>{fields.data(), 1},
                .hostCalls = {},
                .customOpcodes = {},
                .planHints = {},
                .captures = nullptr,
                .capturesOwner = {}
            }
        ).toVector();
        AKK_TEST_CHECK(arithmeticFiltered.size() == 1);
        AKK_TEST_CHECK(arithmeticFiltered[0].value.id == 3);

        bc::CustomOpcodeRegistry customRegistry;
        std::ostringstream customOpcodeLog;
        AKK_TEST_CHECK(!customRegistry.registerOpcode(
            bc::CustomOpcodeBinding{
                .opcode = 1,
                .name = "bad_user_range",
                .arity = 1,
                .resultKind = bc::ValueKind::Bool,
                .thunk = &oddAgeOpcode
            },
            &customOpcodeLog
        ));
        AKK_TEST_CHECK(customOpcodeLog.str().find("rejected custom opcode") != std::string::npos);
        AKK_TEST_CHECK(customRegistry.registerOpcode(
            bc::CustomOpcodeBinding{
                .opcode = bc::customOpcodeUserMin,
                .name = "odd_age",
                .arity = 1,
                .resultKind = bc::ValueKind::Bool,
                .thunk = &oddAgeOpcode
            },
            &customOpcodeLog
        ));
        AKK_TEST_CHECK(!customRegistry.registerOpcode(
            bc::CustomOpcodeBinding{
                .opcode = bc::customOpcodeUserMin,
                .name = "odd_age_duplicate",
                .arity = 1,
                .resultKind = bc::ValueKind::Bool,
                .thunk = &oddAgeOpcode
            },
            &customOpcodeLog
        ));
        static constexpr std::array<uint8_t, 7> customBytecode{
            static_cast<uint8_t>(bc::Opcode::LoadField), 0, 0,
            static_cast<uint8_t>(bc::Opcode::CallCustom), 0, 0x80,
            static_cast<uint8_t>(bc::Opcode::Return)
        };
        auto customFiltered = profiles.__query_compiled(
            bc::CompiledQueryDescriptor<Profile>{
                .code = customBytecode,
                .constants = {},
                .fields = std::span<const bc::FieldBinding<Profile>>{fields.data(), 1},
                .hostCalls = {},
                .customOpcodes = customRegistry.bindings(),
                .planHints = {},
                .captures = nullptr,
                .capturesOwner = {}
            }
        ).toVector();
        AKK_TEST_CHECK(customFiltered.size() == 3);

        static constexpr std::array<uint8_t, 21> shortCircuitBytecode{
            static_cast<uint8_t>(bc::Opcode::LoadField), 0, 0,
            static_cast<uint8_t>(bc::Opcode::PushConst), 0, 0,
            static_cast<uint8_t>(bc::Opcode::Gt),
            static_cast<uint8_t>(bc::Opcode::JumpIfFalse), 17, 0,
            static_cast<uint8_t>(bc::Opcode::LoadField), 0, 0,
            static_cast<uint8_t>(bc::Opcode::CallCustom), 1, 0x80,
            static_cast<uint8_t>(bc::Opcode::Return),
            static_cast<uint8_t>(bc::Opcode::PushConst), 1, 0,
            static_cast<uint8_t>(bc::Opcode::Return)
        };
        static constexpr std::array<bc::Value, 2> shortCircuitConstants{
            bc::Value::uinteger(100),
            bc::Value::boolean(false)
        };
        static constexpr std::array<bc::CustomOpcodeBinding, 1> rejectingCustomOpcode{
            bc::CustomOpcodeBinding{
                .opcode = bc::customOpcodeUserMin + 1,
                .name = "rejecting",
                .arity = 1,
                .resultKind = bc::ValueKind::Bool,
                .thunk = &rejectingOpcode
            }
        };
        auto shortCircuitFiltered = profiles.__query_compiled(
            bc::CompiledQueryDescriptor<Profile>{
                .code = shortCircuitBytecode,
                .constants = shortCircuitConstants,
                .fields = std::span<const bc::FieldBinding<Profile>>{fields.data(), 1},
                .hostCalls = {},
                .customOpcodes = rejectingCustomOpcode,
                .planHints = {},
                .captures = nullptr,
                .capturesOwner = {}
            }
        ).toVector();
        AKK_TEST_CHECK(shortCircuitFiltered.empty());

        static constexpr std::array<uint8_t, 4> hostBytecode{
            static_cast<uint8_t>(bc::Opcode::HostCallBool), 0, 0,
            static_cast<uint8_t>(bc::Opcode::Return)
        };
        static constexpr std::array<bc::HostCallThunk<Profile>, 1> hostCalls{
            &profileIsBobbyWithOddAge
        };

        auto hostFiltered = profiles.__query_compiled(
            bc::CompiledQueryDescriptor<Profile>{
                .code = hostBytecode,
                .constants = {},
                .fields = {},
                .hostCalls = hostCalls,
                .customOpcodes = {},
                .planHints = {},
                .captures = nullptr,
                .capturesOwner = {}
            }
        ).toVector();
        AKK_TEST_CHECK(hostFiltered.size() == 1);
        AKK_TEST_CHECK(hostFiltered[0].value.id == 2);

        static constexpr std::array<bc::HostCallThunk<Profile>, 1> minAgeHostCalls{
            &profileAgeAtLeastCapture
        };
        static constexpr std::array<bc::HostCallThunk<Profile>, 1> nameHostCalls{
            &profileNameEqualsCapture
        };
        auto minAgeCapture = std::make_shared<ProfileMinAgeCapture>(ProfileMinAgeCapture{18});
        auto nameCapture = std::make_shared<ProfileNameCapture>(ProfileNameCapture{"Bobby"});
        auto composedIndependentHostCaptures = profiles.__query_compiled(
            bc::CompiledQueryDescriptor<Profile>{
                .code = hostBytecode,
                .constants = {},
                .fields = {},
                .hostCalls = minAgeHostCalls,
                .customOpcodes = {},
                .planHints = {},
                .captures = minAgeCapture.get(),
                .capturesOwner = minAgeCapture
            }
        ).where(
            bc::CompiledQueryDescriptor<Profile>{
                .code = hostBytecode,
                .constants = {},
                .fields = {},
                .hostCalls = nameHostCalls,
                .customOpcodes = {},
                .planHints = {},
                .captures = nameCapture.get(),
                .capturesOwner = nameCapture
            }
        ).toVector();
        AKK_TEST_CHECK(composedIndependentHostCaptures.size() == 1);
        AKK_TEST_CHECK(composedIndependentHostCaptures[0].value.id == 2);

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

    void testWideQueryableMacro() {
        using namespace akkaradb;
        TempDir dir{"wideQueryable"};
        auto db = AkkaraDB::open(dir.path, StartupMode::ULTRA_FAST);
        auto rows = db->table<&WideQueryable::id>("wide_queryable");

        rows.put({1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, "target"});
        rows.put({2, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, "other"});

        auto selected = rows.query([](auto row) {
            return row.f10 == 10U && row.label == "target";
        }).toVector();
        AKK_TEST_CHECK(selected.size() == 1);
        AKK_TEST_CHECK(selected[0].value.id == 1);
    }

    void testRefLazyResolveAndCascadePut() {
        using namespace akkaradb;
        TempDir dir{"refLazyResolve"};
        auto db = AkkaraDB::open(dir.path, StartupMode::ULTRA_FAST);
        auto schema = db->schema()
            .table<&RefAuthor::id>("authors")
            .table<&RefPost::id>("posts")
            .table<&PlainPost::id>("plain_posts")
            .table<&RestrictedPost::id>("restricted_posts")
            .table<&NullablePost::id>("nullable_posts")
            .foreignKey<&RefPost::author>({OnDelete::Cascade}, {OnUpdate::Cascade})
            .foreignKey<&PlainPost::authorId, &RefAuthor::id>({}, {OnUpdate::Cascade})
            .foreignKey<&RestrictedPost::authorId, &RefAuthor::id>({OnDelete::Restrict}, {OnUpdate::Restrict})
            .foreignKey<&NullablePost::authorId, &RefAuthor::id>({OnDelete::SetNull}, {OnUpdate::SetNull})
            .open();
        auto& authors = schema.table<RefAuthor>();
        auto& posts = schema.table<RefPost>();
        auto& plainPosts = schema.table<PlainPost>();
        auto& restrictedPosts = schema.table<RestrictedPost>();
        auto& nullablePosts = schema.table<NullablePost>();

        authors.put({1, "Alice", 30, "alice@example.test"});
        posts.put({100, ref<RefAuthor>(1), "hello", 5, "first"});
        nullablePosts.put({400, 1, "set null child", 9});
        nullablePosts.put({401, std::nullopt, "already null", 1});

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

        plainPosts.put({200, 1, 30, "plain pk join", 7});
        plainPosts.put({201, 1, 30, "plain field join", 2});

        auto plainPkJoined = plainPosts.join<&PlainPost::authorId, &RefAuthor::id>(authors).toVector();
        AKK_TEST_CHECK(plainPkJoined.size() == 2);
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

        RefPost postFromValue{101, RefAuthor{20, "Bob", 40, "bob@example.test"}, "from value", 3, "second"};
        AKK_TEST_CHECK(postFromValue.author.dirty());
        AKK_TEST_CHECK(postFromValue.author.loaded());
        posts.put(postFromValue);
        auto bob = authors.get(20);
        AKK_TEST_CHECK(authors.exists(20));
        AKK_TEST_CHECK(bob.has_value());
        AKK_TEST_CHECK(bob->name == "Bob");
        restrictedPosts.put({300, 20, "restrict child", 1});

        authors.put({3, "Carol", 28, "carol@example.test"});
        posts.put({103, ref<RefAuthor>(3), "stable ref", 4, "third"});
        authors.updatePrimaryKey(3, RefAuthor{30, "Carol", 29, "carol@renamed.test"});
        AKK_TEST_CHECK(!authors.exists(3));
        AKK_TEST_CHECK(authors.exists(30));
        auto movedRefPost = posts.get(103);
        AKK_TEST_CHECK(movedRefPost.has_value());
        AKK_TEST_CHECK(movedRefPost->author.id() == 30);
        AKK_TEST_CHECK(movedRefPost->author->email == "carol@renamed.test");

        auto movedJoin = posts.join<&RefPost::author>(authors).where([](const RefPost& post, const RefAuthor& author) {
            return post.id == 103 && author.id == 30;
        }).first();
        AKK_TEST_CHECK(movedJoin.has_value());
        AKK_TEST_CHECK(movedJoin->right.name == "Carol");

        bool missingPlainAuthorRejected = false;
        try {
            plainPosts.put({202, 999, 18, "missing plain fk", 0});
        }
        catch (const std::runtime_error&) {
            missingPlainAuthorRejected = true;
        }
        AKK_TEST_CHECK(missingPlainAuthorRejected);
        AKK_TEST_CHECK(!plainPosts.exists(202));

        authors.updatePrimaryKey(1, RefAuthor{10, "Alice Updated", 31, "alice@updated.test"});
        AKK_TEST_CHECK(!authors.exists(1));
        AKK_TEST_CHECK(authors.exists(10));
        auto cascadedRefPost = posts.get(100);
        AKK_TEST_CHECK(cascadedRefPost.has_value());
        AKK_TEST_CHECK(cascadedRefPost->author.id() == 10);
        auto cascadedPlainPost = plainPosts.get(200);
        AKK_TEST_CHECK(cascadedPlainPost.has_value());
        AKK_TEST_CHECK(cascadedPlainPost->authorId == 10);
        auto nulledOnUpdatePost = nullablePosts.get(400);
        AKK_TEST_CHECK(nulledOnUpdatePost.has_value());
        AKK_TEST_CHECK(!nulledOnUpdatePost->authorId.has_value());

        authors.remove(10);
        AKK_TEST_CHECK(!authors.exists(10));
        AKK_TEST_CHECK(!posts.exists(100));
        AKK_TEST_CHECK(plainPosts.exists(200));
        auto alreadyNullPost = nullablePosts.get(401);
        AKK_TEST_CHECK(alreadyNullPost.has_value());
        AKK_TEST_CHECK(!alreadyNullPost->authorId.has_value());

        bool restrictUpdateRejected = false;
        try {
            authors.updatePrimaryKey(20, RefAuthor{21, "Bob", 41, "bob-renamed@example.test"});
        }
        catch (const std::runtime_error&) {
            restrictUpdateRejected = true;
        }
        AKK_TEST_CHECK(restrictUpdateRejected);
        AKK_TEST_CHECK(authors.exists(20));
        AKK_TEST_CHECK(!authors.exists(21));
        auto unchangedRestricted = restrictedPosts.get(300);
        AKK_TEST_CHECK(unchangedRestricted.has_value());
        AKK_TEST_CHECK(unchangedRestricted->authorId == 20);

        bool restrictRejected = false;
        try {
            authors.remove(20);
        }
        catch (const std::runtime_error&) {
            restrictRejected = true;
        }
        AKK_TEST_CHECK(restrictRejected);
        AKK_TEST_CHECK(authors.exists(20));
        AKK_TEST_CHECK(posts.exists(101));
        AKK_TEST_CHECK(restrictedPosts.exists(300));

        restrictedPosts.remove(300);
        authors.remove(20);
        AKK_TEST_CHECK(!authors.exists(20));
        AKK_TEST_CHECK(!posts.exists(101));
        authors.remove(30);
        AKK_TEST_CHECK(!authors.exists(30));
        AKK_TEST_CHECK(!posts.exists(103));

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

    void testImmutableFields() {
        using namespace akkaradb;
        TempDir dir{"immutableFields"};
        auto db = AkkaraDB::open(dir.path, StartupMode::ULTRA_FAST);
        auto users = db->table<&ImmutableUser::id>("immutable_users");
        users.indexed<&ImmutableUser::handle>();

        ImmutableUser created{1, "alice", 30, "first"};
        users.put(created);

        auto loaded = users.get(1);
        AKK_TEST_CHECK(loaded.has_value());
        AKK_TEST_CHECK(loaded->handle.get() == "alice");

        bool directMutationRejected = false;
        try {
            loaded->handle = "alice-renamed";
        }
        catch (const std::runtime_error&) {
            directMutationRejected = true;
        }
        AKK_TEST_CHECK(directMutationRejected);

        loaded->bio = "updated";
        users.put(*loaded);
        auto afterMutableUpdate = users.get(1);
        AKK_TEST_CHECK(afterMutableUpdate.has_value());
        AKK_TEST_CHECK(afterMutableUpdate->bio == "updated");
        AKK_TEST_CHECK(afterMutableUpdate->handle.get() == "alice");

        bool persistedMutationRejected = false;
        try {
            users.put({1, "alice-renamed", 31, "second"});
        }
        catch (const std::runtime_error&) {
            persistedMutationRejected = true;
        }
        AKK_TEST_CHECK(persistedMutationRejected);

        bool upsertMutationRejected = false;
        try {
            users.upsert(1, [](ImmutableUser& user) {
                user.handle = "alice-upsert";
            });
        }
        catch (const std::runtime_error&) {
            upsertMutationRejected = true;
        }
        AKK_TEST_CHECK(upsertMutationRejected);

        auto indexed = users.findBy<&ImmutableUser::handle>(akkaradb::Immutable<std::string>{"alice"});
        AKK_TEST_CHECK(indexed.has_value());
        AKK_TEST_CHECK(indexed->id == 1);
    }

    void testFieldUpdateHooks() {
        using namespace akkaradb;
        TempDir dir{"fieldUpdateHooks"};
        auto db = AkkaraDB::open(dir.path, StartupMode::ULTRA_FAST);
        auto profiles = db->table<&Profile::id>("update_hook_profiles");

        std::vector<std::string> events;
        profiles.onUpdate<&Profile::age>([&](const auto& oldAge, const auto& newAge, const Profile& oldProfile, const Profile& newProfile) {
            events.push_back(std::format("{}:{}->{}:{}", oldProfile.id, oldAge, newAge, newProfile.name));
        });
        profiles.onUpdate<&Profile::age>([](const Profile& oldProfile, Profile& newProfile) {
            newProfile.email = std::format("age-{}@example.test", newProfile.age);
            newProfile.name = std::format("{} v{}", oldProfile.name, newProfile.age);
        });

        profiles.put({1, "a@example.test", "Alice", 30});
        AKK_TEST_CHECK(events.empty());

        profiles.put({1, "a@example.test", "Alice", 31});
        AKK_TEST_CHECK(events.size() == 1);
        AKK_TEST_CHECK(events[0] == "1:30->31:Alice");
        auto ageUpdated = profiles.get(1);
        AKK_TEST_CHECK(ageUpdated.has_value());
        AKK_TEST_CHECK(ageUpdated->email == "age-31@example.test");
        AKK_TEST_CHECK(ageUpdated->name == "Alice v31");

        profiles.put({1, "renamed@example.test", "Alice Updated", 31});
        AKK_TEST_CHECK(events.size() == 1);
        auto nonAgeUpdated = profiles.get(1);
        AKK_TEST_CHECK(nonAgeUpdated.has_value());
        AKK_TEST_CHECK(nonAgeUpdated->email == "renamed@example.test");
        AKK_TEST_CHECK(nonAgeUpdated->name == "Alice Updated");
    }
} // namespace

int main() {
    akkaradb::test::installMsvcTestErrorHandlers();
    try {
        testTrivialCrud();
        testBinpackRoundtrip();
        testNonUniqueIndexAndCleanup();
        testCountAndScanAreTableScoped();
        testPrimaryKeyRangeScanUsesNumericOrder();
        testSpecv4QueryAndHelpers();
        testNonUnsignedRangeQueryUsesIndexSource();
        testOptionalNullQueryHelpers();
        testNestedFieldQueryHelpers();
        testMapGetQueryHelpers();
        testWideQueryableMacro();
        testRefLazyResolveAndCascadePut();
        testImmutableFields();
        testFieldUpdateHooks();
        return 0;
    }
    catch (const std::exception& ex) {
        akkaradb::test::failFastExit("STD EXCEPTION", ex.what());
    }
    catch (...) {
        akkaradb::test::failFastExit("UNKNOWN EXCEPTION");
    }
}

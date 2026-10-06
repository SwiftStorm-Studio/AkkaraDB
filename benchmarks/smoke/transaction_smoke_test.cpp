/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// benchmarks/smoke/transaction_smoke_test.cpp
#include "akk/engine/AkkEngine.hpp"
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#ifndef _WIN32
#include <sys/wait.h>
#endif

using namespace akkaradb::engine;
namespace fs = std::filesystem;
static std::span<const uint8_t> bytes(const std::string& text) { return {reinterpret_cast<const uint8_t*>(text.data()), text.size()}; }
static std::string text(const std::optional<std::vector<uint8_t>>& value) { return value ? std::string(value->begin(), value->end()) : "<missing>"; }
static void require(bool condition, const char* message) { if (!condition) { throw std::runtime_error(message); } }
static AkkEngineOptions memoryOptions() {
    AkkEngineOptions options;
    options.components.walEnabled = false; options.components.sstEnabled = false;
    options.components.manifestEnabled = false; options.components.blobEnabled = false;
    options.components.versionLogEnabled = false; options.components.clusterEnabled = false;
    options.memtable.shardCount = 2; options.runtime.relaxedConcurrentWrites = true;
    return options;
}
static AkkEngineOptions persistentOptions(const fs::path& dir, int mode = 0) {
    auto options = memoryOptions(); options.paths.dataDir = dir;
    options.components.walEnabled = mode != 1; options.components.sstEnabled = mode != 2;
    options.components.manifestEnabled = mode != 2; options.components.versionLogEnabled = mode != 3;
    options.components.blobEnabled = true; options.blob.thresholdBytes = 8;
    options.blob.gcOnFlush = true; options.blob.gcOnClose = true;
    options.wal.shardCount = 1; options.memtable.thresholdBytesPerShard = 128;
    options.transactions.maxJournalBytes = 4096;
    return options;
}
static void conflict(AkkEngine::Transaction& tx) {
    bool rejected = false; try { tx.end(); } catch (const TransactionConflict&) { rejected = true; }
    require(rejected && !tx.active(), "conflict did not abort transaction");
}
static void testSemantics() {
    auto engine = AkkEngine::open(memoryOptions());
    engine->put(bytes("a"), bytes("one")); engine->put(bytes("c"), bytes("three"));
    auto tx = engine->begin(); require(text(tx.get(bytes("a"))) == "one", "initial snapshot lookup");
    tx.put(bytes("a"), bytes("changed")); tx.put(bytes("b"), bytes("two")); tx.remove(bytes("c"));
    require(text(engine->get(bytes("a"))) == "one" && !engine->exists(bytes("b")), "uncommitted write escaped");
    require(text(tx.get(bytes("a"))) == "changed" && !tx.exists(bytes("c")) && tx.count() == 2, "read own writes");
    akkaradb::core::BufferArena arena{4096}; std::string keys;
    for (const auto& row : tx.scan(arena)) { keys.append(row.key.begin(), row.key.end()); }
    require(keys == "ab", "scan did not merge staged changes in byte order");
    tx.end(); require(!tx.active() && text(engine->get(bytes("a"))) == "changed" && engine->count() == 2, "commit results");
    { auto abandoned = engine->begin(); abandoned.put(bytes("a"), bytes("discard")); }
    require(text(engine->get(bytes("a"))) == "changed", "destructor did not cancel");
    auto cancelled = engine->begin(); cancelled.remove(bytes("a")); cancelled.rollback();
    require(engine->exists(bytes("a")), "explicit rollback changed engine");
    auto snapshot = engine->begin({TransactionIsolation::SNAPSHOT_ISOLATION});
    engine->put(bytes("a"), bytes("later"));
    require(text(snapshot.get(bytes("a"))) == "changed", "snapshot changed after normal write"); snapshot.end();
    auto serial = engine->begin(); require(!serial.exists(bytes("missing")), "missing read");
    engine->put(bytes("missing"), bytes("temporary")); engine->remove(bytes("missing")); conflict(serial);
    auto writer = engine->begin({TransactionIsolation::SNAPSHOT_ISOLATION}); writer.put(bytes("a"), bytes("collision"));
    engine->put(bytes("a"), bytes("winner")); conflict(writer);
    require(text(engine->get(bytes("a"))) == "winner", "conflicting write escaped");
    auto independent = engine->begin(); (void)independent.get(bytes("a")); engine->put(bytes("z"), bytes("unrelated")); independent.end();
    for (auto isolation : {TransactionIsolation::SERIALIZABLE, TransactionIsolation::SNAPSHOT_ISOLATION}) {
        engine->put(bytes("doctor:a"), bytes("on")); engine->put(bytes("doctor:b"), bytes("on"));
        auto first = engine->begin({isolation}); auto second = engine->begin({isolation});
        for (auto* current : {&first, &second}) { require(text(current->get(bytes("doctor:a"))) == "on" && text(current->get(bytes("doctor:b"))) == "on", "write-skew setup"); }
        first.put(bytes("doctor:a"), bytes("off")); second.put(bytes("doctor:b"), bytes("off")); first.end();
        if (isolation == TransactionIsolation::SERIALIZABLE) { conflict(second); }
        else { second.end(); require(text(engine->get(bytes("doctor:b"))) == "off", "snapshot isolation rejected disjoint writes"); }
    }
    auto range = engine->begin(); const auto before = range.count(bytes("range:"), bytes("range;"));
    engine->put(bytes("range:new"), bytes("phantom"));
    require(range.count(bytes("range:"), bytes("range;")) == before, "range snapshot gained phantom"); conflict(range);
    auto bounded = engine->begin(); (void)bounded.count(bytes("range:"), bytes("range;"));
    engine->put(bytes("range;"), bytes("exclusive end")); bounded.end();
    auto moved = engine->begin(); moved.put(bytes("thread"), bytes("handoff"));
    auto worker = std::async(std::launch::async, [tx = std::move(moved)]() mutable { require(text(tx.get(bytes("thread"))) == "handoff", "thread handoff"); tx.end(); });
    worker.get(); engine->close();
}
static void testAtomicReaders() {
    auto engine = AkkEngine::open(memoryOptions());
    { auto tx = engine->begin(); tx.put(bytes("left"), bytes("0")); tx.put(bytes("right"), bytes("0")); tx.end(); }
    const std::string left = "left", right = "right";
    const std::array<std::span<const uint8_t>, 2> keys{bytes(left), bytes(right)};
    std::atomic<bool> stop = false, torn = false; std::string firstTorn;
    std::thread reader([&] { while (!stop.load()) { const auto values = engine->getBatch(keys); if (values[0].value != values[1].value && !torn.exchange(true)) { firstTorn = std::string(values[0].value.begin(), values[0].value.end()) + "/" + std::string(values[1].value.begin(), values[1].value.end()); } } });
    for (int i = 1; i <= 100; ++i) { auto tx = engine->begin(); const auto value = std::to_string(i); tx.put(bytes("left"), bytes(value)); tx.put(bytes("right"), bytes(value)); tx.end(); }
    stop = true; reader.join(); if (torn.load()) { throw std::runtime_error("getBatch observed partial transaction: " + firstTorn); } engine->close();
}
static void testCloseWaitsForCommit() {
    for (int repeat = 0; repeat < 8; ++repeat) {
        auto engine = AkkEngine::open(memoryOptions()); auto tx = engine->begin();
        tx.put(bytes("close"), bytes("committed"));
        auto closing = std::async(std::launch::async, [&] { engine->close(); });
        tx.end(); closing.get(); require(!tx.active(), "concurrent close did not wait for commit publication");
    }
}
static void testLimits() {
    auto options = memoryOptions(); options.transactions.maxOpen = 1; options.transactions.maxWriteBytes = 1024;
    auto engine = AkkEngine::open(options); auto tx = engine->begin();
    bool rejected = false; try { auto extra = engine->begin(); } catch (const std::runtime_error&) { rejected = true; }
    require(rejected, "open limit ignored");
    rejected = false; try { tx.put(bytes("large"), bytes(std::string(2048, 'x'))); } catch (const std::length_error&) { rejected = true; }
    require(rejected && !tx.exists(bytes("large")), "oversized staging was not atomic"); tx.rollback();
    auto next = engine->begin(); next.end(); engine->close();
}
static void setWalFailure(const char* value) {
#ifdef _WIN32
    _putenv_s("AKKARADB_TEST_WAL_FAIL_AFTER_ENTRIES", value);
#else
    if (*value) { setenv("AKKARADB_TEST_WAL_FAIL_AFTER_ENTRIES", value, 1); }
    else { unsetenv("AKKARADB_TEST_WAL_FAIL_AFTER_ENTRIES"); }
#endif
}
static void testStorageFailure(const fs::path& dir) {
    auto options = persistentOptions(dir); options.wal.execution = wal::WalExecutionMode::INLINE;
    setWalFailure("1"); auto engine = AkkEngine::open(options); setWalFailure("");
    auto tx = engine->begin(); tx.put(bytes("a"), bytes("committed-a-value")); tx.put(bytes("b"), bytes("committed-b-value"));
    bool unknown = false; try { tx.end(); } catch (const TransactionOutcomeUnknown&) { unknown = true; }
    require(unknown && !tx.active(), "storage failure did not terminate with unknown outcome");
    bool blocked = false; try { (void)engine->get(bytes("a")); } catch (const std::runtime_error&) { blocked = true; }
    require(blocked, "failed engine exposed a partial commit");
    try { engine->close(); } catch (const std::runtime_error&) {}
    engine.reset(); engine = AkkEngine::open(options);
    require(text(engine->get(bytes("a"))) == "committed-a-value" && text(engine->get(bytes("b"))) == "committed-b-value", "close discarded redo for a partially applied commit"); engine->close();
}
static void testPersistentSemantics(const fs::path& dir) {
    auto engine = AkkEngine::open(persistentOptions(dir));
    engine->put(bytes("a"), bytes("old-a-value")); engine->put(bytes("b"), bytes("old-b-value"));
    auto snapshot = engine->begin({TransactionIsolation::SNAPSHOT_ISOLATION});
    engine->put(bytes("a"), bytes("later-a-value")); engine->forceFlush();
    auto worker = std::async(std::launch::async, [tx = std::move(snapshot)]() mutable {
        require(text(tx.get(bytes("a"))) == "old-a-value", "Blob snapshot did not survive thread handoff and flush"); tx.end();
    }); worker.get();
    auto tx = engine->begin(); tx.put(bytes("a"), bytes("discarded-staging"));
    tx.put(bytes("a"), bytes("committed-a-value")); tx.put(bytes("b"), bytes("committed-b-value")); tx.end();
    uint64_t aSeq = 0, bSeq = 0; size_t aCount = 0;
    for (const auto& entry : engine->history(bytes("a"))) { aSeq = std::max(aSeq, entry.seq); ++aCount; }
    for (const auto& entry : engine->history(bytes("b"))) { bSeq = std::max(bSeq, entry.seq); }
    require(aSeq == bSeq && aCount == 3, "transaction history did not share one atomic revision or coalesce staging");
    require(text(engine->getAt(bytes("a"), aSeq - 1)) == "later-a-value" && text(engine->getAt(bytes("b"), aSeq - 1)) == "old-b-value", "historical cut saw a partial commit");
    require(text(engine->getAt(bytes("a"), aSeq)) == "committed-a-value" && text(engine->getAt(bytes("b"), aSeq)) == "committed-b-value", "historical commit cut missed writes");
    // Exhaust the journal byte budget so a checkpoint runs while commit holds
    // Blob preparation admission; flush callbacks must never wait on that lock.
    for (int i = 0; i < 30; ++i) {
        auto batch = engine->begin(); batch.put(bytes("a"), bytes("journal-a-value")); batch.put(bytes("b"), bytes("journal-b-value")); batch.end();
    }
    engine->close(); engine = AkkEngine::open(persistentOptions(dir));
    require(text(engine->get(bytes("a"))) == "journal-a-value" && text(engine->get(bytes("b"))) == "journal-b-value", "journal checkpoint lost data"); engine->close();
}
static void seed(const fs::path& dir, int mode) {
    auto engine = AkkEngine::open(persistentOptions(dir, mode));
    engine->put(bytes("a"), bytes("old-a-value")); engine->put(bytes("b"), bytes("old-b-value")); engine->put(bytes("gone"), bytes("old-gone-value")); engine->close();
}
static void crashChild(const fs::path& dir, const std::string& point, int mode) {
    auto engine = AkkEngine::open(persistentOptions(dir, mode));
#ifdef _WIN32
    _putenv_s("AKKARADB_TEST_CRASH_POINT", point.c_str());
#else
    setenv("AKKARADB_TEST_CRASH_POINT", point.c_str(), 1);
#endif
    auto tx = engine->begin(); tx.put(bytes("a"), bytes("new-a-value")); tx.put(bytes("b"), bytes("new-b-value")); tx.remove(bytes("gone")); tx.end();
    std::quick_exit(87);
}
static void testRecovery(const fs::path& executable, const fs::path& root) {
    for (int mode = 0; mode != 4; ++mode) {
        for (const std::string point : {"transaction.before_decision", "transaction.after_decision", "transaction.after_apply_record", "transaction.after_publish"}) {
            const auto dir = root / (std::to_string(mode) + point); seed(dir, mode);
            const std::string args = "\"" + executable.string() + "\" --crash \"" + dir.string() + "\" " + point + " " + std::to_string(mode);
#ifdef _WIN32
            const int code = std::system(("\"" + args + "\"").c_str()); require(code == 86, "child did not stop at requested boundary");
#else
            const int code = std::system(args.c_str()); require(WIFEXITED(code) && WEXITSTATUS(code) == 86, "child did not stop at requested boundary");
#endif
            for (int repeat = 0; repeat < 2; ++repeat) {
                auto engine = AkkEngine::open(persistentOptions(dir, mode));
                const bool committed = point != "transaction.before_decision";
                require(text(engine->get(bytes("a"))) == (committed ? "new-a-value" : "old-a-value"), "recovered first key is wrong");
                require(text(engine->get(bytes("b"))) == (committed ? "new-b-value" : "old-b-value"), "recovered second key is wrong");
                require(engine->exists(bytes("gone")) != committed, "recovered deletion is wrong");
                if (mode != 3) {
                    size_t count = 0; for (const auto& entry : engine->history(bytes("a"))) { (void)entry; ++count; }
                    require(count == (committed ? 2u : 1u), "history lost or duplicated a revision during recovery");
                }
                auto tx = engine->begin(); tx.put(bytes("after"), bytes("different-blob-contents")); tx.end();
                require(text(engine->get(bytes("after"))) == "different-blob-contents", "aborted Blob id was reused"); engine->close();
            }
        }
    }
}
int main(int argc, char** argv) {
    try {
        if (argc == 2 && std::string{argv[1]} == "--concurrency") { for (int i = 0; i < 20; ++i) { testAtomicReaders(); } return 0; }
        if (argc == 5 && std::string{argv[1]} == "--crash") { crashChild(argv[2], argv[3], std::stoi(argv[4])); }
        testSemantics(); testAtomicReaders(); testCloseWaitsForCommit(); testLimits();
        const auto root = fs::temp_directory_path() / ("akk-transactions-test-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        testStorageFailure(root / "storage-failure");
        testPersistentSemantics(root / "persistent-semantics");
        testRecovery(fs::absolute(argv[0]), root); fs::remove_all(root);
        std::cout << "transaction tests passed\n"; return 0;
    } catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}

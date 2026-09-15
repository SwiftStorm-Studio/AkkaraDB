/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// benchmarks/smoke/cluster_smoke_test.cpp
#ifdef _WIN32
#include <winsock2.h>
#else
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#endif
#include "TestErrorHandlers.hpp"

#include "akk/core/record/MemHdr16.hpp"
#include "akk/engine/AkkEngine.hpp"
#include "akk/engine/blob/BlobManager.hpp"
#include "akk/engine/cluster/ClusterConfig.hpp"
#include "akk/engine/cluster/ClusterManager.hpp"
#include "akk/engine/cluster/ClusterRuntime.hpp"
#include "akk/engine/cluster/ClusterRouter.hpp"
#include "akk/engine/cluster/ReplFraming.hpp"
#include "akk/engine/cluster/ReplicationClient.hpp"
#include "akk/engine/cluster/ReplicationServer.hpp"
#include "akk/engine/cluster/detail/ReplicationTransfer.hpp"
#include "akk/engine/erasure/ErasureCodec.hpp"
#include "akk/engine/erasure/ErasureCodecExt.hpp"
#include "akk/engine/manifest/Manifest.hpp"
#include "akk/engine/memtable/MemTable.hpp"
#include "akk/engine/wal/WalFraming.hpp"
#include "akk/engine/wal/WalRecovery.hpp"
#include "akk/engine/wal/WalWriter.hpp"
#include "akk/cpu/CRC32C.hpp"
#include "akk/crypto/Identity.hpp"

#include <array>
#include <atomic>
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <limits>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

using namespace akkaradb::engine::cluster;
namespace erasure = akkaradb::engine::erasure;
namespace blob = akkaradb::engine::blob;
namespace manifest = akkaradb::engine::manifest;
namespace memtable = akkaradb::engine::memtable;
namespace wal = akkaradb::engine::wal;

extern "C" bool akkaradb_cluster_register() noexcept;

namespace {
    constexpr uint32_t DATA_AND_COORDINATOR =
        static_cast<uint32_t>(NodeCapability::DATA_BEARING) |
        static_cast<uint32_t>(NodeCapability::COORDINATOR_ELIGIBLE);

    [[noreturn]] void fail(const char* expr, const char* file, int line) {
        std::fprintf(stderr, "check failed: %s at %s:%d\n", expr, file, line);
        throw std::runtime_error(std::string{"check failed: "} + expr + " at " + file + ":" + std::to_string(line));
    }

#define AKK_CLUSTER_CHECK(expr) do { if (!(expr)) { fail(#expr, __FILE__, __LINE__); } } while (false)

    std::filesystem::path makeTempDir(const std::string& suffix) {
        const auto dir = std::filesystem::temp_directory_path() / ("akkaradbClusterTest_" + suffix);
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
        std::filesystem::create_directories(dir, ec);
        AKK_CLUSTER_CHECK(!ec);
        return dir;
    }

    NodeInfo node(uint64_t id, uint16_t dataPort, uint16_t replPort, uint32_t capabilities = DATA_AND_COORDINATOR) {
        return NodeInfo{
            .nodeId = id,
            .host = "127.0.0.1",
            .dataPort = dataPort,
            .replPort = replPort,
            .capabilities = capabilities,
        };
    }

    RaftOptions onlineRaftMembership() {
        RaftOptions raft;
        raft.membership.mode = RaftMembershipMode::JOINT_CONSENSUS;
        raft.membership.allowOnlineVoterChanges = true;
        return raft;
    }

    std::span<const uint8_t> bytesOf(const std::string& value) {
        return {reinterpret_cast<const uint8_t*>(value.data()), value.size()};
    }

    std::string textOf(std::span<const uint8_t> value) {
        return {reinterpret_cast<const char*>(value.data()), value.size()};
    }

    std::vector<uint64_t> nodeIdsOf(const std::vector<NodeInfo>& nodes) {
        std::vector<uint64_t> ids;
        ids.reserve(nodes.size());
        for (const auto& node : nodes) { ids.push_back(node.nodeId); }
        return ids;
    }

    struct TestGroupState {
        uint64_t groupId = 0;
        uint64_t primaryNodeId = 0;
        uint64_t groupEpoch = 0;
    };

    TestGroupState readGroupState(const std::filesystem::path& path) {
        std::ifstream in(path, std::ios::binary);
        std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        AKK_CLUSTER_CHECK(bytes.size() == 33);
        AKK_CLUSTER_CHECK((std::string_view{reinterpret_cast<const char*>(bytes.data()), 5} == "AKCG2"));
        auto readU32 = [](const std::vector<uint8_t>& data, size_t off) {
            return static_cast<uint32_t>(data[off]) |
                   (static_cast<uint32_t>(data[off + 1]) << 8) |
                   (static_cast<uint32_t>(data[off + 2]) << 16) |
                   (static_cast<uint32_t>(data[off + 3]) << 24);
        };
        auto readU64 = [](const std::vector<uint8_t>& data, size_t off) {
            uint64_t out = 0;
            for (size_t i = 0; i < 8; ++i) { out |= static_cast<uint64_t>(data[off + i]) << (i * 8); }
            return out;
        };
        auto crcBytes = bytes;
        for (size_t i = 29; i < 33; ++i) { crcBytes[i] = 0; }
        AKK_CLUSTER_CHECK(readU32(bytes, 29) == akkaradb::cpu::CRC32C(
            reinterpret_cast<const std::byte*>(crcBytes.data()),
            crcBytes.size()
        ));
        return TestGroupState{
            .groupId = readU64(bytes, 5),
            .primaryNodeId = readU64(bytes, 13),
            .groupEpoch = readU64(bytes, 21),
        };
    }

    void writeTextFile(const std::filesystem::path& path, const std::string& text) {
        if (path.has_parent_path()) { std::filesystem::create_directories(path.parent_path()); }
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        AKK_CLUSTER_CHECK(out);
        out << text;
        AKK_CLUSTER_CHECK(out);
    }

    void writeNodeIdFile(const std::filesystem::path& path, uint64_t nodeId) {
        if (path.has_parent_path()) { std::filesystem::create_directories(path.parent_path()); }
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        AKK_CLUSTER_CHECK(out);
        out.write(reinterpret_cast<const char*>(&nodeId), sizeof(nodeId));
        AKK_CLUSTER_CHECK(out);
    }

    void appendTextFile(const std::filesystem::path& path, const std::string& text) {
        std::ofstream out(path, std::ios::binary | std::ios::app);
        AKK_CLUSTER_CHECK(out);
        out << text;
        AKK_CLUSTER_CHECK(out);
    }

    void corruptFileByte(const std::filesystem::path& path, std::streamoff offset = 0) {
        std::fstream file{path, std::ios::binary | std::ios::in | std::ios::out};
        AKK_CLUSTER_CHECK(file);
        file.seekg(offset);
        char byte = 0;
        file.get(byte);
        AKK_CLUSTER_CHECK(file);
        byte = static_cast<char>(static_cast<unsigned char>(byte) ^ 0x5Au);
        file.clear();
        file.seekp(offset);
        file.put(byte);
        file.flush();
        AKK_CLUSTER_CHECK(file);
    }

    void enableDeterministicRaftElection() {
#ifdef _WIN32
        AKK_CLUSTER_CHECK(_putenv_s("AKKARADB_TEST_DETERMINISTIC_RAFT_ELECTION", "1") == 0);
#else
        AKK_CLUSTER_CHECK(::setenv("AKKARADB_TEST_DETERMINISTIC_RAFT_ELECTION", "1", 1) == 0);
#endif
    }

    void setRequestJournalCompactionTestThreshold(const char* value) {
#ifdef _WIN32
        AKK_CLUSTER_CHECK(_putenv_s("AKKARADB_TEST_REQUEST_JOURNAL_COMPACT_RECORDS", value == nullptr ? "" : value) == 0);
#else
        if (value == nullptr) { AKK_CLUSTER_CHECK(::unsetenv("AKKARADB_TEST_REQUEST_JOURNAL_COMPACT_RECORDS") == 0); }
        else { AKK_CLUSTER_CHECK(::setenv("AKKARADB_TEST_REQUEST_JOURNAL_COMPACT_RECORDS", value, 1) == 0); }
#endif
    }

    class ScopedRequestJournalCompactionThreshold {
        public:
            explicit ScopedRequestJournalCompactionThreshold(const char* value) { setRequestJournalCompactionTestThreshold(value); }
            ~ScopedRequestJournalCompactionThreshold() { setRequestJournalCompactionTestThreshold(nullptr); }
            ScopedRequestJournalCompactionThreshold(const ScopedRequestJournalCompactionThreshold&) = delete;
            ScopedRequestJournalCompactionThreshold& operator=(const ScopedRequestJournalCompactionThreshold&) = delete;
    };

    void setLeaseRenewalFailureTestEnvironment(bool enabled) {
#ifdef _WIN32
        AKK_CLUSTER_CHECK(_putenv_s("AKKARADB_TEST_LEASE_RENEW_INTERVAL_MS", enabled ? "10" : "") == 0);
        AKK_CLUSTER_CHECK(_putenv_s("AKKARADB_TEST_FAIL_LEASE_RENEWAL", enabled ? "1" : "") == 0);
#else
        if (enabled) {
            AKK_CLUSTER_CHECK(::setenv("AKKARADB_TEST_LEASE_RENEW_INTERVAL_MS", "10", 1) == 0);
            AKK_CLUSTER_CHECK(::setenv("AKKARADB_TEST_FAIL_LEASE_RENEWAL", "1", 1) == 0);
        }
        else {
            AKK_CLUSTER_CHECK(::unsetenv("AKKARADB_TEST_LEASE_RENEW_INTERVAL_MS") == 0);
            AKK_CLUSTER_CHECK(::unsetenv("AKKARADB_TEST_FAIL_LEASE_RENEWAL") == 0);
        }
#endif
    }

    class ScopedLeaseRenewalFailure {
        public:
            ScopedLeaseRenewalFailure() { setLeaseRenewalFailureTestEnvironment(true); }
            ~ScopedLeaseRenewalFailure() { setLeaseRenewalFailureTestEnvironment(false); }
            ScopedLeaseRenewalFailure(const ScopedLeaseRenewalFailure&) = delete;
            ScopedLeaseRenewalFailure& operator=(const ScopedLeaseRenewalFailure&) = delete;
    };

    void setSnapshotStagingCorruptionPoint(const char* point) {
#ifdef _WIN32
        AKK_CLUSTER_CHECK(_putenv_s("AKKARADB_TEST_CORRUPT_SNAPSHOT_STAGING_POINT", point == nullptr ? "" : point) == 0);
#else
        if (point == nullptr) {
            AKK_CLUSTER_CHECK(::unsetenv("AKKARADB_TEST_CORRUPT_SNAPSHOT_STAGING_POINT") == 0);
        }
        else {
            AKK_CLUSTER_CHECK(::setenv("AKKARADB_TEST_CORRUPT_SNAPSHOT_STAGING_POINT", point, 1) == 0);
        }
#endif
    }

    class ScopedSnapshotStagingCorruption {
        public:
            explicit ScopedSnapshotStagingCorruption(const char* point) { setSnapshotStagingCorruptionPoint(point); }
            ~ScopedSnapshotStagingCorruption() { clear(); }

            ScopedSnapshotStagingCorruption(const ScopedSnapshotStagingCorruption&) = delete;
            ScopedSnapshotStagingCorruption& operator=(const ScopedSnapshotStagingCorruption&) = delete;

            void clear() {
                if (!active_) { return; }
                setSnapshotStagingCorruptionPoint(nullptr);
                active_ = false;
            }

        private:
            bool active_ = true;
    };

    void writeU32Le(std::vector<uint8_t>& out, size_t offset, uint32_t value) {
        for (size_t i = 0; i < 4; ++i) { out[offset + i] = static_cast<uint8_t>(value >> (i * 8)); }
    }

    uint64_t readU64Le(std::span<const uint8_t> bytes, size_t offset) {
        uint64_t out = 0;
        for (size_t i = 0; i < 8; ++i) { out |= static_cast<uint64_t>(bytes[offset + i]) << (i * 8); }
        return out;
    }

    uint64_t readRaftLogLastIncludedIndex(const std::filesystem::path& path) {
        std::ifstream in(path, std::ios::binary);
        if (!in) { return 0; }
        std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        if (bytes.size() < 5) { return 0; }
        const std::string_view magic{reinterpret_cast<const char*>(bytes.data()), 5};
        if (magic == "AKRL1" && bytes.size() >= 49) { return readU64Le(bytes, 21); }
        return 0;
    }

    uint64_t nextPrng(uint64_t& state) noexcept {
        state = state * 6364136223846793005ull + 1442695040888963407ull;
        return state;
    }

    [[nodiscard]] uint64_t nowUs() noexcept {
        return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count());
    }

    std::vector<uint8_t> deterministicBytes(size_t size, uint64_t seed) {
        std::vector<uint8_t> out(size);
        uint64_t state = seed;
        for (auto& byte : out) { byte = static_cast<uint8_t>(nextPrng(state) >> 56u); }
        return out;
    }

    std::vector<erasure::ErasureShard> selectShards(
        const std::vector<erasure::ErasureShard>& shards,
        erasure::ErasureLayout layout,
        uint64_t seed
    ) {
        std::vector<uint16_t> indices;
        indices.reserve(shards.size());
        for (uint16_t i = 0; i < layout.totalShards(); ++i) { indices.push_back(i); }

        uint64_t state = seed;
        for (size_t i = indices.size(); i > 1; --i) {
            const size_t j = static_cast<size_t>(nextPrng(state) % i);
            std::swap(indices[i - 1], indices[j]);
        }

        std::vector<erasure::ErasureShard> selected;
        selected.reserve(layout.dataShards);
        for (uint16_t i = 0; i < layout.dataShards; ++i) { selected.push_back(shards[indices[i]]); }
        return selected;
    }

    template <typename Predicate>
    bool waitUntil(Predicate predicate, std::chrono::milliseconds timeout = std::chrono::milliseconds{5000}) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (predicate()) { return true; }
            std::this_thread::sleep_for(std::chrono::milliseconds{25});
        }
        return predicate();
    }

    struct RuntimeHarness {
        uint64_t nodeId = 0;
        std::atomic<uint64_t> lastSeq{0};
        std::atomic<uint64_t> appliedCount{0};
        std::atomic<uint64_t> durableCount{0};
        std::atomic<uint64_t> snapshotsExported{0};
        std::atomic<uint64_t> snapshotsInstalled{0};
        mutable std::mutex stateMutex;
        std::string lastKey;
        std::string lastValue;
        uint64_t lastBlobSeq = 0;
        uint64_t lastBlobId = 0;
        std::string lastBlobContent;
        std::string pendingSnapshotKey;
        std::string pendingSnapshotValue;
        std::filesystem::path durableStatePath;
        std::unique_ptr<ClusterRuntime> runtime;

        void persistDurableStateLocked() const {
            if (durableStatePath.empty()) { return; }
            std::filesystem::create_directories(durableStatePath.parent_path());
            const auto tmp = durableStatePath.parent_path() / (durableStatePath.filename().string() + ".tmp");
            std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
            AKK_CLUSTER_CHECK(out.good());
            const char magic[] = {'A', 'K', 'H', 'S', '1'};
            out.write(magic, sizeof(magic));
            const auto writeU64 = [&](uint64_t value) {
                for (size_t i = 0; i < 8; ++i) { out.put(static_cast<char>(value >> (i * 8))); }
            };
            const auto writeString = [&](const std::string& value) {
                writeU64(value.size());
                out.write(value.data(), static_cast<std::streamsize>(value.size()));
            };
            writeU64(lastSeq.load());
            writeString(lastKey);
            writeString(lastValue);
            writeU64(lastBlobSeq);
            writeU64(lastBlobId);
            writeString(lastBlobContent);
            out.flush();
            AKK_CLUSTER_CHECK(out.good());
            out.close();
            std::error_code ec;
            std::filesystem::rename(tmp, durableStatePath, ec);
            if (ec) {
                std::filesystem::remove(durableStatePath, ec);
                ec.clear();
                std::filesystem::rename(tmp, durableStatePath, ec);
            }
            AKK_CLUSTER_CHECK(!ec);
        }

        void loadDurableState() {
            if (durableStatePath.empty()) { return; }
            std::ifstream in(durableStatePath, std::ios::binary);
            if (!in) { return; }
            const auto readU64 = [&]() -> uint64_t {
                uint64_t value = 0;
                for (size_t i = 0; i < 8; ++i) {
                    const int ch = in.get();
                    if (ch == EOF) { throw std::runtime_error("cluster smoke: truncated harness state"); }
                    value |= static_cast<uint64_t>(static_cast<uint8_t>(ch)) << (i * 8);
                }
                return value;
            };
            const auto readString = [&]() -> std::string {
                const uint64_t size = readU64();
                if (size > 1024 * 1024) { throw std::runtime_error("cluster smoke: oversized harness state"); }
                std::string value(static_cast<size_t>(size), '\0');
                in.read(value.data(), static_cast<std::streamsize>(value.size()));
                if (!in) { throw std::runtime_error("cluster smoke: truncated harness state string"); }
                return value;
            };
            char magic[5]{};
            in.read(magic, sizeof(magic));
            if (!in || std::string_view{magic, sizeof(magic)} != "AKHS1") { return; }
            lastSeq.store(readU64());
            lastKey = readString();
            lastValue = readString();
            lastBlobSeq = readU64();
            lastBlobId = readU64();
            lastBlobContent = readString();
        }

        void setState(uint64_t seq, std::span<const uint8_t> key, std::span<const uint8_t> value) {
            std::lock_guard lock{stateMutex};
            lastKey = textOf(key);
            lastValue = textOf(value);
            lastSeq.store(seq);
            persistDurableStateLocked();
        }

        bool hasState(uint64_t seq, const std::string& key, const std::string& value) const {
            std::lock_guard lock{stateMutex};
            return lastSeq.load() == seq && lastKey == key && lastValue == value;
        }

        bool hasBlob(uint64_t seq, uint64_t blobId, const std::string& content) const {
            std::lock_guard lock{stateMutex};
            return lastBlobSeq == seq && lastBlobId == blobId && lastBlobContent == content;
        }
    };

    std::unique_ptr<RuntimeHarness> makeRuntime(
        const std::filesystem::path& dir,
        const ClusterConfig& cfg,
        uint64_t nodeId,
        ClusterRuntimeOptions options = {},
        bool secure = false
    ) {
        auto harness = std::make_unique<RuntimeHarness>();
        harness->nodeId = nodeId;
        harness->durableStatePath = dir / "harness-state.bin";
        harness->loadDurableState();
        options.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
        ClusterEngineCallbacks callbacks;
        callbacks.getLastSeq = [harness = harness.get()] { return harness->lastSeq.load(); };
        callbacks.getCurrentSeq = [harness = harness.get()] { return harness->lastSeq.load(); };
        callbacks.read = [harness = harness.get()](std::span<const uint8_t> key, uint64_t) {
            std::lock_guard lock{harness->stateMutex};
            ReadResponse response;
            response.status = textOf(key) == harness->lastKey ? ReadStatus::FOUND : ReadStatus::NOT_FOUND;
            response.seq = harness->lastSeq.load();
            response.value.assign(harness->lastValue.begin(), harness->lastValue.end());
            return response;
        };
        callbacks.exportSnapshot = [harness = harness.get()]() -> std::optional<ClusterSnapshot> {
            harness->snapshotsExported.fetch_add(1);
            std::lock_guard lock{harness->stateMutex};
            const uint64_t seq = harness->lastSeq.load();
            if (seq == 0) { return std::nullopt; }
            ClusterSnapshot snapshot;
            snapshot.seq = seq;
            snapshot.entryCount = 1;
            const auto entry = std::make_shared<ClusterHistoryEntry>(ClusterHistoryEntry{
                    .seq = seq,
                    .sourceNodeId = harness->nodeId,
                    .op = static_cast<uint8_t>(ReplOpType::PUT),
                    .recordFlags = 0,
                    .key = std::vector<uint8_t>{harness->lastKey.begin(), harness->lastKey.end()},
                    .value = std::vector<uint8_t>{harness->lastValue.begin(), harness->lastValue.end()},
                });
            snapshot.forEachEntry = [entry](const ClusterSnapshot::EntryVisitor& visitor) {
                return visitor(entry->key, entry->value);
            };
            return snapshot;
        };
        callbacks.beginSnapshot = [harness = harness.get()](uint64_t, uint64_t) {
            std::lock_guard lock{harness->stateMutex};
            harness->pendingSnapshotKey.clear();
            harness->pendingSnapshotValue.clear();
        };
        callbacks.beginSnapshotEntry = [harness = harness.get()](std::span<const uint8_t> key, uint64_t, uint32_t) {
            std::lock_guard lock{harness->stateMutex};
            harness->pendingSnapshotKey = textOf(key);
            harness->pendingSnapshotValue.clear();
        };
        callbacks.appendSnapshotEntryChunk = [harness = harness.get()](uint64_t, std::span<const uint8_t> chunk) {
            std::lock_guard lock{harness->stateMutex};
            harness->pendingSnapshotValue += textOf(chunk);
        };
        callbacks.finishSnapshotEntry = [] {};
        callbacks.finishSnapshot = [harness = harness.get()](uint64_t snapshotSeq) {
            std::lock_guard lock{harness->stateMutex};
            harness->lastKey = harness->pendingSnapshotKey;
            harness->lastValue = harness->pendingSnapshotValue;
            harness->lastSeq.store(snapshotSeq);
            harness->snapshotsInstalled.fetch_add(1);
            harness->persistDurableStateLocked();
        };
        callbacks.apply = [harness = harness.get()](
            uint64_t seq,
            ReplOpType,
            std::span<const uint8_t> key,
            std::span<const uint8_t> value,
            uint8_t,
            uint64_t
        ) {
            harness->setState(seq, key, value);
            harness->appliedCount.fetch_add(1);
        };
        callbacks.beginBlob = [harness = harness.get()](uint64_t seq, uint64_t blobId, uint64_t, uint32_t) {
            std::lock_guard lock{harness->stateMutex};
            harness->lastBlobSeq = seq;
            harness->lastBlobId = blobId;
            harness->lastBlobContent.clear();
        };
        callbacks.appendBlobChunk = [harness = harness.get()](uint64_t, uint64_t, uint64_t, std::span<const uint8_t> chunk) {
            std::lock_guard lock{harness->stateMutex};
            harness->lastBlobContent += textOf(chunk);
        };
        callbacks.finishBlob = [harness = harness.get()](uint64_t, uint64_t) {
            std::lock_guard lock{harness->stateMutex};
            harness->persistDurableStateLocked();
        };
        callbacks.abortBlob = [harness = harness.get()](uint64_t, uint64_t) {
            std::lock_guard lock{harness->stateMutex};
            harness->lastBlobContent.clear();
        };
        callbacks.forceDurable = [harness = harness.get()] { harness->durableCount.fetch_add(1); };
        harness->runtime = ClusterRuntime::create(dir, cfg, nodeId, std::move(callbacks), options);
        return harness;
    }

    RuntimeHarness* findLeader(std::span<RuntimeHarness* const> nodes) {
        RuntimeHarness* leader = nullptr;
        for (auto* n : nodes) {
            if (n->runtime->role() != NodeRole::PRIMARY) { continue; }
            if (leader != nullptr) { return nullptr; }
            leader = n;
        }
        return leader;
    }

    RuntimeHarness* findNodeById(std::span<RuntimeHarness* const> nodes, uint64_t nodeId) {
        for (auto* n : nodes) {
            if (n->nodeId == nodeId) { return n; }
        }
        return nullptr;
    }

    uint64_t preferredLeaderId(std::span<RuntimeHarness* const> nodes) {
        uint64_t out = std::numeric_limits<uint64_t>::max();
        for (const auto* n : nodes) { out = std::min(out, n->nodeId); }
        AKK_CLUSTER_CHECK(out != std::numeric_limits<uint64_t>::max());
        return out;
    }

    bool isRetryableLeaderChange(const std::runtime_error& error) {
        const std::string message = error.what();
        return message.find("local node is not Raft leader") != std::string::npos ||
               message.find("leadership changed") != std::string::npos ||
               message.find("failed to replicate entry to Raft majority") != std::string::npos ||
               message.find("failed to replicate Blob payload to Raft majority") != std::string::npos ||
               message.find("prior entry is uncommitted") != std::string::npos ||
               message.find("membership change already in progress") != std::string::npos;
    }

    template <typename Fn>
    void runOnLeader(std::span<RuntimeHarness* const> nodes, Fn&& fn, std::chrono::milliseconds timeout = std::chrono::milliseconds{30000}) {
        const auto deadline = std::chrono::steady_clock::now() + std::max(timeout, std::chrono::milliseconds{20000});
        std::optional<std::runtime_error> lastRetryable;
        while (std::chrono::steady_clock::now() < deadline) {
            RuntimeHarness* leader = findLeader(nodes);
            if (leader == nullptr) {
                std::this_thread::sleep_for(std::chrono::milliseconds{25});
                continue;
            }
            try {
                fn(*leader);
                return;
            }
            catch (const std::runtime_error& error) {
                if (!isRetryableLeaderChange(error)) { throw; }
                lastRetryable = error;
                std::this_thread::sleep_for(std::chrono::milliseconds{50});
            }
        }
        if (lastRetryable) { throw *lastRetryable; }
        throw std::runtime_error("cluster smoke: no Raft leader became available");
    }

    void ensureLeader(
        std::span<RuntimeHarness* const> nodes,
        uint64_t targetNodeId,
        std::chrono::milliseconds timeout = std::chrono::milliseconds{30000}
    ) {
        RuntimeHarness* target = findNodeById(nodes, targetNodeId);
        AKK_CLUSTER_CHECK(target != nullptr);

        const auto deadline = std::chrono::steady_clock::now() + std::max(timeout, std::chrono::milliseconds{20000});
        std::optional<std::runtime_error> lastRetryable;
        while (std::chrono::steady_clock::now() < deadline) {
            RuntimeHarness* leader = findLeader(nodes);
            if (leader == target) { return; }
            if (leader == nullptr) {
                std::this_thread::sleep_for(std::chrono::milliseconds{25});
                continue;
            }
            try {
                leader->runtime->transferRaftLeadership(targetNodeId);
                if (waitUntil([&] { return findLeader(nodes) == target; }, std::chrono::milliseconds{5000})) { return; }
            }
            catch (const std::runtime_error& error) {
                if (!isRetryableLeaderChange(error)) { throw; }
                lastRetryable = error;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{50});
        }
        if (lastRetryable) { throw *lastRetryable; }
        throw std::runtime_error("cluster smoke: failed to establish deterministic Raft leader");
    }

    void ensurePreferredLeader(
        std::span<RuntimeHarness* const> nodes,
        std::chrono::milliseconds timeout = std::chrono::milliseconds{30000}
    ) {
        ensureLeader(nodes, preferredLeaderId(nodes), timeout);
    }

    akkaradb::engine::AkkEngine* findEngineLeader(std::span<akkaradb::engine::AkkEngine* const> nodes) {
        akkaradb::engine::AkkEngine* leader = nullptr;
        for (auto* engine : nodes) {
            if (engine->stats().cluster.role != static_cast<uint32_t>(NodeRole::PRIMARY)) { continue; }
            if (leader != nullptr) { return nullptr; }
            leader = engine;
        }
        return leader;
    }

    template <typename Fn>
    void runOnEngineLeader(
        std::span<akkaradb::engine::AkkEngine* const> nodes,
        Fn&& fn,
        std::chrono::milliseconds timeout = std::chrono::milliseconds{30000}
    ) {
        const auto deadline = std::chrono::steady_clock::now() + std::max(timeout, std::chrono::milliseconds{20000});
        std::optional<std::runtime_error> lastRetryable;
        while (std::chrono::steady_clock::now() < deadline) {
            akkaradb::engine::AkkEngine* leader = findEngineLeader(nodes);
            if (leader == nullptr) {
                std::this_thread::sleep_for(std::chrono::milliseconds{25});
                continue;
            }
            try {
                fn(*leader);
                return;
            }
            catch (const std::runtime_error& error) {
                if (!isRetryableLeaderChange(error)) { throw; }
                lastRetryable = error;
                std::this_thread::sleep_for(std::chrono::milliseconds{50});
            }
        }
        if (lastRetryable) { throw *lastRetryable; }
        throw std::runtime_error("cluster smoke: no Raft engine leader became available");
    }

    std::optional<std::vector<uint8_t>> engineGet(akkaradb::engine::AkkEngine& engine, const std::string& key) {
        return engine.get(bytesOf(key));
    }

    std::string keyOwnedBy(const ClusterConfig& cfg, uint64_t nodeId, const std::string& prefix) {
        ClusterRouter router{cfg};
        for (uint32_t i = 0; i < 10000; ++i) {
            std::string key = prefix + "-" + std::to_string(i);
            const auto targets = router.writeTargets(bytesOf(key));
            if (!targets.empty() && targets.front().nodeId == nodeId) { return key; }
        }
        throw std::runtime_error("cluster smoke: failed to find partitioned owner key");
    }

    [[nodiscard]] std::vector<uint8_t> snapshotCommitValue(uint64_t recordCount) {
        std::vector<uint8_t> out;
        out.insert(out.end(), {'A', 'K', 'S', 'C', '1'});
        for (size_t i = 0; i < sizeof(uint64_t); ++i) { out.push_back(static_cast<uint8_t>(recordCount >> (i * 8))); }
        return out;
    }

    [[nodiscard]] std::optional<std::vector<uint8_t>> memtableGet(memtable::MemTable& table, const std::string& key, uint64_t seq) {
        std::vector<uint8_t> out;
        const auto hit = table.getInto(bytesOf(key), seq, out);
        if (!hit.has_value() || !*hit) { return std::nullopt; }
        return out;
    }

    void testWalSnapshotTransactionRecovery() {
        const auto dir = makeTempDir("wal-snapshot-transaction");
        const std::string oldKey = "snapshot-old-key";
        const std::string keepKey = "snapshot-keep-key";
        const std::string newKey = "snapshot-new-key";
        const std::string oldValue = "old-value";
        const std::string keepValue = "keep-value";
        const std::string newValue = "new-value";

        auto writeBase = [&](const std::filesystem::path& walDir) {
            auto writer = wal::WalWriter::create(walDir);
            writer->append(bytesOf(oldKey), bytesOf(oldValue), 1, akkaradb::core::MemHdr16::FLAG_NORMAL);
            writer->append(bytesOf(keepKey), bytesOf(oldValue), 2, akkaradb::core::MemHdr16::FLAG_NORMAL);
            writer->close();
        };
        auto recoverTable = [](const std::filesystem::path& walDir) {
            auto table = memtable::MemTable::create();
            (void)wal::WalRecovery::recoverInto(wal::WalRecoveryOptions{.walDir = walDir}, *table);
            return table;
        };

        const auto uncommittedWal = dir / "uncommitted";
        writeBase(uncommittedWal);
        {
            auto writer = wal::WalWriter::create(uncommittedWal);
            writer->append(
                bytesOf(oldKey),
                {},
                5,
                wal::WAL_FLAG_SNAPSHOT_RECORD | akkaradb::core::MemHdr16::FLAG_TOMBSTONE
            );
            writer->append(bytesOf(newKey), bytesOf(newValue), 5, wal::WAL_FLAG_SNAPSHOT_RECORD);
            writer->close();
        }
        {
            auto table = recoverTable(uncommittedWal);
            const auto oldRecovered = memtableGet(*table, oldKey, 5);
            const auto newRecovered = memtableGet(*table, newKey, 5);
            AKK_CLUSTER_CHECK(oldRecovered.has_value() && textOf(*oldRecovered) == oldValue);
            AKK_CLUSTER_CHECK(!newRecovered.has_value());
        }

        const auto committedWal = dir / "committed";
        writeBase(committedWal);
        {
            auto writer = wal::WalWriter::create(committedWal);
            writer->append(
                bytesOf(oldKey),
                {},
                5,
                wal::WAL_FLAG_SNAPSHOT_RECORD | akkaradb::core::MemHdr16::FLAG_TOMBSTONE
            );
            writer->append(bytesOf(keepKey), bytesOf(keepValue), 5, wal::WAL_FLAG_SNAPSHOT_RECORD);
            writer->append(bytesOf(newKey), bytesOf(newValue), 5, wal::WAL_FLAG_SNAPSHOT_RECORD);
            const auto commit = snapshotCommitValue(3);
            writer->append(bytesOf("AKSC1"), commit, 5, wal::WAL_FLAG_SNAPSHOT_COMMIT, 0, wal::WalAppendAck::SYNCED);
            writer->close();
        }
        {
            auto table = recoverTable(committedWal);
            const auto oldRecovered = memtableGet(*table, oldKey, 5);
            const auto keepRecovered = memtableGet(*table, keepKey, 5);
            const auto newRecovered = memtableGet(*table, newKey, 5);
            AKK_CLUSTER_CHECK(!oldRecovered.has_value());
            AKK_CLUSTER_CHECK(keepRecovered.has_value() && textOf(*keepRecovered) == keepValue);
            AKK_CLUSTER_CHECK(newRecovered.has_value() && textOf(*newRecovered) == newValue);
        }
    }

    void setStorageCrashPoint(const char* point) {
#ifdef _WIN32
        AKK_CLUSTER_CHECK(_putenv_s("AKKARADB_TEST_CRASH_POINT", point) == 0);
#else
        AKK_CLUSTER_CHECK(::setenv("AKKARADB_TEST_CRASH_POINT", point, 1) == 0);
#endif
    }

    void runStorageCrashChild(const std::filesystem::path& executable, const char* mode, const char* point, const std::filesystem::path& dir) {
        std::string command = "\"" + executable.string() + "\" " + mode + " \"" + point + "\" \"" + dir.string() + "\"";
#ifdef _WIN32
        command = "\"" + command + "\"";
        constexpr int expected = 86;
#else
        constexpr int expected = 86 << 8;
#endif
        AKK_CLUSTER_CHECK(std::system(command.c_str()) == expected);
    }

    ClusterConfig stripeStorageConfig() {
        return ClusterConfig{
            {node(1, 21801, 21901), node(2, 21802, 21902)},
            ReplicationMode::STRIPE, AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::ASYNC, .ackTimeoutMs = 400},
            {}, StripeOptions{.dataShards = 1, .parityShards = 1},
        };
    }

    akkaradb::engine::AkkEngineOptions stripeStorageOptions(const std::filesystem::path& dir, uint64_t id) {
        akkaradb::engine::AkkEngineOptions options;
        options.paths.dataDir = dir / ("n" + std::to_string(id));
        options.components.clusterEnabled = true;
        options.components.blobEnabled = false;
        options.cluster.config = stripeStorageConfig();
        options.cluster.runtime.transportMode = TransportMode::PLAIN;
        options.cluster.runtime.clusterGroupId = 9180;
        options.cluster.runtime.clusterGroupEpoch = 1;
        options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        options.cluster.runtime.stripeReadCoordinatorMode = StripeReadCoordinatorMode::LOCAL_COORDINATOR;
        writeNodeIdFile(options.paths.dataDir / "node.id", id);
        return options;
    }

    int runStripeCrashWriter(const char* point, const std::filesystem::path& dir) {
        auto n1 = akkaradb::engine::AkkEngine::open(stripeStorageOptions(dir, 1));
        auto n2 = akkaradb::engine::AkkEngine::open(stripeStorageOptions(dir, 2));
        const auto key = keyOwnedBy(stripeStorageConfig(), 1, "stripe-crash");
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { n1->put(bytesOf(key), bytesOf("before")); return true; }
            catch (const std::runtime_error&) { return false; }
        }));
        setStorageCrashPoint(point);
        n1->put(bytesOf(key), bytesOf("after"));
        return 42;
    }

    std::pair<size_t, size_t> stripeInternalCounts(akkaradb::engine::AkkEngine& engine) {
        akkaradb::core::BufferArena arena;
        size_t shards = 0;
        size_t intents = 0;
        for (const auto& record : engine.scan(arena)) {
            if (record.key.size() < 6 || record.key[0] != 0 || record.key[1] != 'A' || record.key[2] != 'K' || record.key[3] != 'S') { continue; }
            if (record.key[4] == 'S') { ++shards; }
            if (record.key[4] == 'T') { ++intents; }
        }
        return {shards, intents};
    }

    void testStripePublicationCrashRecovery(const std::filesystem::path& executable) {
        const auto key = keyOwnedBy(stripeStorageConfig(), 1, "stripe-crash");
        for (const char* point : {"stripe.after_intent", "stripe.after_shard", "stripe.before_publish", "stripe.after_publish"}) {
            const auto dir = makeTempDir(point);
            runStorageCrashChild(executable, "--stripe-crash-writer", point, dir);
            auto n1 = akkaradb::engine::AkkEngine::open(stripeStorageOptions(dir, 1));
            auto n2 = akkaradb::engine::AkkEngine::open(stripeStorageOptions(dir, 2));
            const std::string expected = std::string_view{point} == "stripe.after_publish" ? "after" : "before";
            AKK_CLUSTER_CHECK(waitUntil([&] {
                try {
                    const auto value = engineGet(*n2, key);
                    return value && textOf(*value) == expected;
                }
                catch (const std::runtime_error&) { return false; }
            }));
            AKK_CLUSTER_CHECK(waitUntil([&] {
                const auto a = stripeInternalCounts(*n1);
                const auto b = stripeInternalCounts(*n2);
                return a.first == 1 && b.first == 1 && a.second == 0 && b.second == 0;
            }, std::chrono::milliseconds{10000}));
            const auto recoveredSeq = n1->stats().currentSeq;
            n1->put(bytesOf(key), bytesOf("next-generation"));
            AKK_CLUSTER_CHECK(n1->stats().currentSeq > recoveredSeq);
            AKK_CLUSTER_CHECK(waitUntil([&] {
                const auto a = stripeInternalCounts(*n1);
                const auto b = stripeInternalCounts(*n2);
                return a.first == 1 && b.first == 1 && a.second == 0;
            }));
            n1->close();
            bool rejected = false;
            try { (void)engineGet(*n2, key); }
            catch (const std::runtime_error&) { rejected = true; }
            AKK_CLUSTER_CHECK(rejected);
            n2->close();
        }
    }

    void testStripeFailureAndConcurrentReads() {
        const auto dir = makeTempDir("stripe-failure-and-concurrency");
        const auto key = keyOwnedBy(stripeStorageConfig(), 1, "stripe-failure");
        auto n1 = akkaradb::engine::AkkEngine::open(stripeStorageOptions(dir, 1));
        auto n2 = akkaradb::engine::AkkEngine::open(stripeStorageOptions(dir, 2));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { n1->put(bytesOf(key), bytesOf("stable")); return true; }
            catch (const std::runtime_error&) { return false; }
        }));
        n2->close();
        n2.reset();
        bool rejected = false;
        try { n1->put(bytesOf(key), bytesOf("must-not-publish")); }
        catch (const std::runtime_error&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected); // ASYNC in the config cannot bypass all-shard durability.
        const auto previous = engineGet(*n1, key);
        AKK_CLUSTER_CHECK(previous && textOf(*previous) == "stable");
        n2 = akkaradb::engine::AkkEngine::open(stripeStorageOptions(dir, 2));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try {
                const auto value = engineGet(*n2, key);
                return value && textOf(*value) == "stable";
            }
            catch (const std::runtime_error&) { return false; }
        }));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto a = stripeInternalCounts(*n1);
            const auto b = stripeInternalCounts(*n2);
            return a.first == 1 && b.first == 1 && a.second == 0;
        }));

        std::atomic<unsigned> successfulReads{0};
        std::atomic<bool> invalidRead{false};
        std::jthread reader([&](std::stop_token stop) {
            while (!stop.stop_requested()) {
                try {
                    const auto value = engineGet(*n2, key);
                    if (!value) { invalidRead.store(true); continue; }
                    const auto text = textOf(*value);
                    if (text != "stable" && text != std::string(4096, 'a') && text != std::string(4096, 'b')) {
                        invalidRead.store(true);
                    }
                    successfulReads.fetch_add(1);
                }
                catch (const std::runtime_error&) {} // Bounded generation-revalidation retries may ask the caller to retry.
            }
        });
        for (unsigned i = 0; i < 12; ++i) {
            try { n1->put(bytesOf(key), bytesOf(std::string(4096, i % 2 == 0 ? 'a' : 'b'))); }
            catch (const std::runtime_error& error) {
                throw std::runtime_error("STRIPE concurrent overwrite " + std::to_string(i) + ": " + error.what());
            }
        }
        AKK_CLUSTER_CHECK(waitUntil([&] { return successfulReads.load() != 0; }));
        reader.request_stop();
        reader.join();
        AKK_CLUSTER_CHECK(!invalidRead.load());
        n1->remove(bytesOf(key));
        AKK_CLUSTER_CHECK(!engineGet(*n2, key));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto a = stripeInternalCounts(*n1);
            const auto b = stripeInternalCounts(*n2);
            return a.first == 0 && b.first == 0 && a.second == 0;
        }, std::chrono::milliseconds{10000}));
        n2->close();
        n1->close();

        auto options = stripeStorageOptions(makeTempDir("stripe-requires-wal"), 1);
        options.components.walEnabled = false;
        rejected = false;
        try { (void)akkaradb::engine::AkkEngine::open(options); }
        catch (const std::invalid_argument&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected);
    }

    ClusterConfig stripeFailoverStorageConfig() {
        constexpr uint32_t failoverCapabilities = DATA_AND_COORDINATOR |
            static_cast<uint32_t>(NodeCapability::STRIPE_FAILOVER_ELIGIBLE);
        return ClusterConfig{
            {node(1, 21821, 21921), node(2, 21822, 21922), node(3, 21823, 21923, failoverCapabilities)},
            ReplicationMode::STRIPE, AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 2000},
            {}, StripeOptions{.dataShards = 1, .parityShards = 1},
        };
    }

    akkaradb::engine::AkkEngineOptions stripeFailoverStorageOptions(const std::filesystem::path& dir, uint64_t id) {
        auto options = stripeStorageOptions(dir, id);
        options.cluster.config = stripeFailoverStorageConfig();
        options.cluster.runtime.clusterGroupId = 9181;
        options.cluster.runtime.clusterGroupEpoch = 1;
        return options;
    }

    void testStripeOwnerFailoverAndHandoff() {
        const auto dir = makeTempDir("stripe-owner-failover");
        const auto config = stripeFailoverStorageConfig();
        const auto key = keyOwnedBy(config, 1, "stripe-failover");
        auto owner = akkaradb::engine::AkkEngine::open(stripeFailoverStorageOptions(dir, 1));
        auto shard = akkaradb::engine::AkkEngine::open(stripeFailoverStorageOptions(dir, 2));
        auto failover = akkaradb::engine::AkkEngine::open(stripeFailoverStorageOptions(dir, 3));

        std::string firstWriteError;
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { owner->put(bytesOf(key), bytesOf("owner-generation")); return true; }
            catch (const std::runtime_error& error) { firstWriteError = error.what(); return false; }
        }, std::chrono::milliseconds{10000}) || (std::fprintf(stderr, "stripe failover initial write: %s\n", firstWriteError.c_str()), false));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try {
                const auto value = engineGet(*shard, key);
                return value && textOf(*value) == "owner-generation";
            }
            catch (const std::runtime_error&) { return false; }
        }, std::chrono::milliseconds{10000}));
        owner->close();
        owner.reset();

        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { failover->put(bytesOf(key), bytesOf("failover-generation")); return true; }
            catch (const std::runtime_error&) { return false; }
        }, std::chrono::milliseconds{10000}));
        const auto duringFailure = engineGet(*failover, key);
        AKK_CLUSTER_CHECK(duringFailure && textOf(*duringFailure) == "failover-generation");

        owner = akkaradb::engine::AkkEngine::open(stripeFailoverStorageOptions(dir, 1));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { owner->put(bytesOf(key), bytesOf("owner-returned")); return true; }
            catch (const std::runtime_error&) { return false; }
        }, std::chrono::milliseconds{10000}));
        const auto afterHandoff = engineGet(*owner, key);
        AKK_CLUSTER_CHECK(afterHandoff && textOf(*afterHandoff) == "owner-returned");

        owner->close();
        shard->close();
        failover->close();
    }


    std::unique_ptr<ClusterRuntime> makeSegmentedRaft(const std::filesystem::path& dir, RaftLogRecoveryAction recovery = RaftLogRecoveryAction::FAIL_STARTUP) {
        const auto configPath = dir / "cluster.cfg";
        const ClusterConfig config = std::filesystem::exists(configPath)
                                         ? ClusterConfig::load(configPath)
                                         : ClusterConfig{
                                               {node(1, 21811, 21911)}, ReplicationMode::MIRROR, AckPolicy{},
                                               ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM},
                                           };
        if (!std::filesystem::exists(configPath)) { ClusterConfig::save(configPath, config); }
        ClusterEngineCallbacks callbacks;
        callbacks.getLastSeq = [] { return uint64_t{0}; };
        callbacks.getCurrentSeq = [] { return uint64_t{0}; };
        callbacks.apply = [](uint64_t, ReplOpType, std::span<const uint8_t>, std::span<const uint8_t>, uint8_t, uint64_t) {};
        callbacks.forceDurable = [] {};
        ClusterRuntimeOptions options;
        options.transportMode = TransportMode::PLAIN;
        options.raftLogRecoveryAction = recovery;
        return ClusterRuntime::create(dir, config, 1, std::move(callbacks), options);
    }

    int runRaftLogCrashWriter(const char* point, const std::filesystem::path& dir) {
        auto runtime = makeSegmentedRaft(dir);
        runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return runtime->role() == NodeRole::PRIMARY; }));
        runtime->shipEntry(1, ReplOpType::PUT, bytesOf("committed"), bytesOf("before"), 0, 1);
        setStorageCrashPoint(point);
        runtime->shipEntry(2, ReplOpType::PUT, bytesOf("pending"), bytesOf("after"), 0, 1);
        return 42;
    }

    std::vector<uint8_t> readTestFile(const std::filesystem::path& path) {
        std::ifstream in(path, std::ios::binary);
        AKK_CLUSTER_CHECK(in.good());
        return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
    }

    void testRaftSegmentedLogRecovery(const std::filesystem::path& executable) {
        for (const char* point : {"raft.after_segment_sync", "raft.after_log_metadata"}) {
            const auto dir = makeTempDir(point);
            runStorageCrashChild(executable, "--raft-log-crash-writer", point, dir);
            const auto metadata = readTestFile(dir / "cluster-raft.log");
            const bool published = std::string_view{point} == "raft.after_log_metadata";
            AKK_CLUSTER_CHECK(readU64Le(metadata, 45) == (published ? 3u : 2u));
            auto runtime = makeSegmentedRaft(dir);
            runtime->start();
            AKK_CLUSTER_CHECK(waitUntil([&] { return runtime->role() == NodeRole::PRIMARY; }));
            runtime->shipEntry(3, ReplOpType::PUT, bytesOf("recovered"), bytesOf("safe"), 0, 1);
            runtime->close();
        }

        const auto dir = makeTempDir("raft-uncommitted-segment-corruption");
        runStorageCrashChild(executable, "--raft-log-crash-writer", "raft.after_log_metadata", dir);
        const auto segment = dir / "cluster-raft.log.segments" / "segment-1.akrl";
        auto bytes = readTestFile(segment);
        {
            std::fstream out(segment, std::ios::binary | std::ios::in | std::ios::out);
            out.seekp(-1, std::ios::end);
            out.put(static_cast<char>(bytes.back() ^ 0x40));
            AKK_CLUSTER_CHECK(out.good());
        }
        bool rejected = false;
        try { (void)makeSegmentedRaft(dir); }
        catch (const std::runtime_error&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected);
        auto recovered = makeSegmentedRaft(dir, RaftLogRecoveryAction::TRUNCATE_UNCOMMITTED_TAIL);
        AKK_CLUSTER_CHECK(readU64Le(readTestFile(dir / "cluster-raft.log"), 45) == 2);
        recovered->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return recovered->role() == NodeRole::PRIMARY; }));
        recovered->shipEntry(2, ReplOpType::PUT, bytesOf("replacement"), bytesOf("valid"), 0, 1);
        recovered->close();
        rejected = false;
        {
            std::fstream out(segment, std::ios::binary | std::ios::in | std::ios::out);
            out.seekp(12);
            out.put(static_cast<char>(bytes[12] ^ 0x40));
            AKK_CLUSTER_CHECK(out.good());
        }
        try { (void)makeSegmentedRaft(dir, RaftLogRecoveryAction::TRUNCATE_UNCOMMITTED_TAIL); }
        catch (const std::runtime_error&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected);

        const auto rotateDir = makeTempDir("raft-segment-rotation");
        auto runtime = makeSegmentedRaft(rotateDir);
        runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return runtime->role() == NodeRole::PRIMARY; }));
        const std::string payload(6 * 1024 * 1024, 'x');
        for (uint64_t seq = 1; seq <= 3; ++seq) {
            runtime->shipEntry(seq, ReplOpType::PUT, bytesOf("large"), bytesOf(payload), 0, 1);
        }
        const auto firstSegment = rotateDir / "cluster-raft.log.segments" / "segment-1.akrl";
        const auto firstBytes = readTestFile(firstSegment);
        const auto modified = std::filesystem::last_write_time(firstSegment);
        runtime->shipEntry(4, ReplOpType::PUT, bytesOf("small"), bytesOf("tail"), 0, 1);
        AKK_CLUSTER_CHECK(readTestFile(firstSegment) == firstBytes);
        AKK_CLUSTER_CHECK(std::filesystem::last_write_time(firstSegment) == modified);
        AKK_CLUSTER_CHECK(std::filesystem::exists(rotateDir / "cluster-raft.log.segments" / "segment-2.akrl"));
        AKK_CLUSTER_CHECK(std::filesystem::file_size(rotateDir / "cluster-raft.log") < 1024);
        runtime->close();
        auto reopened = makeSegmentedRaft(rotateDir);
        reopened->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return reopened->role() == NodeRole::PRIMARY; }));
        reopened->close();
    }


    void testRaftOptionsRoundtripAndValidation() {
        const auto dir = makeTempDir("raft-options");
        const ClusterConfig cfg{
            {
                node(1, 20151, 20251),
                node(2, 20152, 20252),
                node(3, 20153, 20253),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE},
            onlineRaftMembership(),
        };
        const auto path = dir / "cluster.cfg";
        ClusterConfig::save(path, cfg);
        const auto loaded = ClusterConfig::load(path);
        AKK_CLUSTER_CHECK(loaded.primaryNodeId() == 0);
        AKK_CLUSTER_CHECK(loaded.consistency().ackTimeoutAction == AckTimeoutAction::FAIL_WRITE);
        AKK_CLUSTER_CHECK(loaded.raft().membership.mode == RaftMembershipMode::JOINT_CONSENSUS);
        AKK_CLUSTER_CHECK(loaded.raft().membership.allowOnlineVoterChanges);
        AKK_CLUSTER_CHECK(loaded.clusterId() == cfg.clusterId());

        bool rejected = false;
        try {
            (void)ClusterConfig{
                {
                    node(1, 20154, 20254),
                    node(2, 20155, 20255),
                },
                ReplicationMode::MIRROR,
                AckPolicy{},
                ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK},
                onlineRaftMembership(),
            };
        }
        catch (const std::invalid_argument&) {
            rejected = true;
        }
        AKK_CLUSTER_CHECK(rejected);

        const auto rejectsSnapshotPolicy = [&](ClusterRuntimeOptions options) {
            options.transportMode = TransportMode::PLAIN;
            try {
                cfg.validateRuntime(1, options);
                return false;
            }
            catch (const std::invalid_argument&) { return true; }
        };
        ClusterRuntimeOptions invalidEntries;
        invalidEntries.raftSnapshot.minLogEntries = 0;
        AKK_CLUSTER_CHECK(rejectsSnapshotPolicy(invalidEntries));
        ClusterRuntimeOptions invalidBytes;
        invalidBytes.raftSnapshot.minLogBytes = 0;
        AKK_CLUSTER_CHECK(rejectsSnapshotPolicy(invalidBytes));
        ClusterRuntimeOptions invalidInterval;
        invalidInterval.raftSnapshot.maxIntervalMs = 0;
        AKK_CLUSTER_CHECK(rejectsSnapshotPolicy(invalidInterval));
    }

    void testMirrorPrimaryRoundtripAndValidation() {
        const auto dir = makeTempDir("mirror-primary-config");
        const ClusterConfig cfg{
            {
                node(1, 20161, 20261),
                node(2, 20162, 20262),
                node(3, 20163, 20263),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK},
            {},
            {},
            2,
        };
        const auto path = dir / "cluster.cfg";
        ClusterConfig::save(path, cfg);
        const auto loaded = ClusterConfig::load(path);
        AKK_CLUSTER_CHECK(loaded.primaryNodeId() == 2);

        ClusterRuntimeOptions primaryOptions;
        primaryOptions.transportMode = TransportMode::PLAIN;
        primaryOptions.startupRole = NodeStartupRole::PRIMARY;
        auto configuredPrimary = makeRuntime(dir / "n2", loaded, 2, primaryOptions);
        configuredPrimary->runtime->start();
        AKK_CLUSTER_CHECK(configuredPrimary->runtime->role() == NodeRole::PRIMARY);

        auto wrongPrimary = makeRuntime(dir / "n1", loaded, 1, primaryOptions);
        bool rejectedWrongPrimary = false;
        try {
            wrongPrimary->runtime->start();
        }
        catch (const std::runtime_error&) {
            rejectedWrongPrimary = true;
        }
        AKK_CLUSTER_CHECK(rejectedWrongPrimary);
        wrongPrimary->runtime->close();
        configuredPrimary->runtime->close();

        bool rejectedUnknown = false;
        try {
            (void)ClusterConfig{
                {node(1, 20164, 20264), node(2, 20165, 20265)},
                ReplicationMode::MIRROR,
                AckPolicy{},
                ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK},
                {},
                {},
                3,
            };
        }
        catch (const std::invalid_argument&) {
            rejectedUnknown = true;
        }
        AKK_CLUSTER_CHECK(rejectedUnknown);

        bool rejectedIneligible = false;
        try {
            (void)ClusterConfig{
                {
                    node(1, 20166, 20266),
                    node(2, 20167, 20267, NodeCapability::DATA_BEARING),
                },
                ReplicationMode::MIRROR,
                AckPolicy{},
                ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK},
                {},
                {},
                2,
            };
        }
        catch (const std::invalid_argument&) {
            rejectedIneligible = true;
        }
        AKK_CLUSTER_CHECK(rejectedIneligible);

        bool rejectedPartitionedPrimary = false;
        try {
            (void)ClusterConfig{
                {node(1, 20168, 20268), node(2, 20169, 20269)},
                ReplicationMode::PARTITIONED,
                AckPolicy{},
                ConsistencyOptions{},
                {},
                {},
                1,
            };
        }
        catch (const std::invalid_argument&) {
            rejectedPartitionedPrimary = true;
        }
        AKK_CLUSTER_CHECK(rejectedPartitionedPrimary);
    }

    void testReplFramingRejectsOversizedPayloadLength() {
        std::vector<uint8_t> wire(ReplFrameHeader::SIZE);
        writeU32Le(wire, 0, ReplFrameHeader::MAGIC);
        wire[4] = static_cast<uint8_t>(ReplMsgType::ACK);
        writeU32Le(wire, 6, ReplFrameHeader::MAX_PAYLOAD_SIZE + 1);

        DecodedFrame decoded;
        AKK_CLUSTER_CHECK(!decodeFrame(wire, decoded));
    }

    void testStripeControlFraming() {
        StripeControlRequest request{
            .requestId = 17,
            .action = StripeControlAction::COMMIT,
            .ownerNodeId = 3,
            .fenceToken = 99,
            .key = {1, 2, 3},
            .metadata = {4, 5, 6, 7},
        };
        DecodedFrame requestFrame;
        AKK_CLUSTER_CHECK(decodeFrame(encodeStripeControlRequest(request), requestFrame));
        StripeControlRequest decodedRequest;
        AKK_CLUSTER_CHECK(decodeStripeControlRequest(requestFrame.payload, decodedRequest));
        AKK_CLUSTER_CHECK(decodedRequest.requestId == request.requestId);
        AKK_CLUSTER_CHECK(decodedRequest.action == request.action);
        AKK_CLUSTER_CHECK(decodedRequest.ownerNodeId == request.ownerNodeId);
        AKK_CLUSTER_CHECK(decodedRequest.fenceToken == request.fenceToken);
        AKK_CLUSTER_CHECK(decodedRequest.key == request.key);
        AKK_CLUSTER_CHECK(decodedRequest.metadata == request.metadata);

        StripeControlResponse response{
            .requestId = request.requestId,
            .status = StripeControlStatus::GRANTED,
            .authorityNodeId = 8,
            .fenceToken = request.fenceToken,
            .metadata = {9, 10},
        };
        DecodedFrame responseFrame;
        AKK_CLUSTER_CHECK(decodeFrame(encodeStripeControlResponse(response), responseFrame));
        StripeControlResponse decodedResponse;
        AKK_CLUSTER_CHECK(decodeStripeControlResponse(responseFrame.payload, decodedResponse));
        AKK_CLUSTER_CHECK(decodedResponse.requestId == response.requestId);
        AKK_CLUSTER_CHECK(decodedResponse.status == response.status);
        AKK_CLUSTER_CHECK(decodedResponse.authorityNodeId == response.authorityNodeId);
        AKK_CLUSTER_CHECK(decodedResponse.fenceToken == response.fenceToken);
        AKK_CLUSTER_CHECK(decodedResponse.metadata == response.metadata);
    }

    void testReplHandshakeCarriesClusterGroupIdentity() {
        const ClientHello client{
            .nodeId = 2,
            .lastSeq = 42,
            .role = NodeRole::REPLICA,
            .groupId = 0x12345678,
            .groupEpoch = 7,
        };
        DecodedFrame clientFrame;
        AKK_CLUSTER_CHECK(decodeFrame(encodeClientHello(client), clientFrame));
        ClientHello decodedClient;
        AKK_CLUSTER_CHECK(decodeClientHello(clientFrame.payload, decodedClient));
        AKK_CLUSTER_CHECK(decodedClient.nodeId == client.nodeId);
        AKK_CLUSTER_CHECK(decodedClient.lastSeq == client.lastSeq);
        AKK_CLUSTER_CHECK(decodedClient.groupId == client.groupId);
        AKK_CLUSTER_CHECK(decodedClient.groupEpoch == client.groupEpoch);

        const ServerHello server{
            .nodeId = 1,
            .currentSeq = 43,
            .role = NodeRole::PRIMARY,
            .groupId = client.groupId,
            .groupEpoch = client.groupEpoch,
        };
        DecodedFrame serverFrame;
        AKK_CLUSTER_CHECK(decodeFrame(encodeServerHello(server), serverFrame));
        ServerHello decodedServer;
        AKK_CLUSTER_CHECK(decodeServerHello(serverFrame.payload, decodedServer));
        AKK_CLUSTER_CHECK(decodedServer.nodeId == server.nodeId);
        AKK_CLUSTER_CHECK(decodedServer.currentSeq == server.currentSeq);
        AKK_CLUSTER_CHECK(decodedServer.groupId == server.groupId);
        AKK_CLUSTER_CHECK(decodedServer.groupEpoch == server.groupEpoch);
    }

    void testPartitionedAndStripeRouting() {
        const ClusterConfig partitioned{
            {
                node(1, 20301, 20401),
                node(2, 20302, 20402),
                node(3, 20303, 20403),
            },
            ReplicationMode::PARTITIONED,
            AckPolicy{},
        };
        ClusterRouter partitionedRouter{partitioned};
        const auto partitionedTargets = partitionedRouter.writeTargets(bytesOf("partitioned-key"));
        AKK_CLUSTER_CHECK(partitionedTargets.size() == 1);

        StripeOptions stripeOptions;
        stripeOptions.dataShards = 3;
        stripeOptions.parityShards = 2;
        constexpr uint32_t stripeFailoverCapabilities = DATA_AND_COORDINATOR |
            static_cast<uint32_t>(NodeCapability::STRIPE_FAILOVER_ELIGIBLE);
        const ClusterConfig stripe{
            {
                node(1, 20311, 20411),
                node(2, 20312, 20412),
                node(3, 20313, 20413),
                node(4, 20314, 20414),
                node(5, 20315, 20415),
                node(6, 20316, 20416, stripeFailoverCapabilities),
            },
            ReplicationMode::STRIPE,
            AckPolicy{},
            {},
            {},
            stripeOptions,
        };
        ClusterRouter stripeRouter{stripe};
        const auto shardTargets = stripeRouter.stripeShardTargets(bytesOf("stripe-key"));
        AKK_CLUSTER_CHECK(shardTargets.size() == 5);
        std::vector<uint64_t> nodeIds;
        for (size_t i = 0; i < shardTargets.size(); ++i) {
            AKK_CLUSTER_CHECK(shardTargets[i].shardIndex == i);
            nodeIds.push_back(shardTargets[i].node.nodeId);
        }
        std::sort(nodeIds.begin(), nodeIds.end());
        AKK_CLUSTER_CHECK(std::adjacent_find(nodeIds.begin(), nodeIds.end()) == nodeIds.end());

        const auto dir = makeTempDir("stripe-config");
        const auto path = dir / "cluster.cfg";
        ClusterConfig::save(path, stripe);
        const auto loaded = ClusterConfig::load(path);
        AKK_CLUSTER_CHECK(loaded.mode() == ReplicationMode::STRIPE);
        AKK_CLUSTER_CHECK(loaded.stripe().dataShards == 3);
        AKK_CLUSTER_CHECK(loaded.stripe().parityShards == 2);
        AKK_CLUSTER_CHECK(loaded.stripeFailoverNode() && loaded.stripeFailoverNode()->nodeId == 6);

        bool rejectedDuplicateFailover = false;
        try {
            (void)ClusterConfig{
                {
                    node(1, 20321, 20421, stripeFailoverCapabilities),
                    node(2, 20322, 20422, stripeFailoverCapabilities),
                    node(3, 20323, 20423),
                },
                ReplicationMode::STRIPE, AckPolicy{}, {}, {}, StripeOptions{.dataShards = 1, .parityShards = 1},
            };
        }
        catch (const std::invalid_argument&) { rejectedDuplicateFailover = true; }
        AKK_CLUSTER_CHECK(rejectedDuplicateFailover);

        bool rejectedFailoverWithoutSpare = false;
        try {
            (void)ClusterConfig{
                {node(1, 20324, 20424, stripeFailoverCapabilities), node(2, 20325, 20425)},
                ReplicationMode::STRIPE, AckPolicy{}, {}, {}, StripeOptions{.dataShards = 1, .parityShards = 1},
            };
        }
        catch (const std::invalid_argument&) { rejectedFailoverWithoutSpare = true; }
        AKK_CLUSTER_CHECK(rejectedFailoverWithoutSpare);

        const auto rejectsMissingGroupId = [&](const ClusterConfig& config, const std::filesystem::path& runtimeDir) {
            ClusterRuntimeOptions options;
            options.transportMode = TransportMode::PLAIN;
            try {
                auto runtime = makeRuntime(runtimeDir, config, config.nodes().front().nodeId, options);
                return false;
            }
            catch (const std::invalid_argument&) { return true; }
        };
        AKK_CLUSTER_CHECK(rejectsMissingGroupId(partitioned, dir / "partitioned-missing-group"));
        AKK_CLUSTER_CHECK(rejectsMissingGroupId(stripe, dir / "stripe-missing-group"));
    }

    void testStripeRuntimeStartsActivePlacement() {
        const auto dir = makeTempDir("stripe-runtime-active-placement");

        StripeOptions stripeOptions;
        stripeOptions.dataShards = 2;
        stripeOptions.parityShards = 1;
        const ClusterConfig stripe{
            {
                node(1, 20318, 20418),
                node(2, 20319, 20419),
                node(3, 20320, 20420),
            },
            ReplicationMode::STRIPE,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 500},
            {},
            stripeOptions,
        };

        ClusterRuntimeOptions options;
        options.transportMode = TransportMode::PLAIN;
        options.clusterGroupId = 9010;
        options.clusterGroupEpoch = 1;

        auto runtime = makeRuntime(dir / "stripe", stripe, 1, options);
        runtime->runtime->start();
        AKK_CLUSTER_CHECK(runtime->runtime->role() == NodeRole::PRIMARY);
        runtime->runtime->close();
    }

    void testPartitionedRuntimeOwnerOnlyReplication() {
        const auto dir = makeTempDir("partitioned-runtime-owner");
        const ClusterConfig cfg{
            {
                node(1, 21601, 21701),
                node(2, 21602, 21702),
            },
            ReplicationMode::PARTITIONED,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 500},
        };

        ClusterRuntimeOptions options;
        options.transportMode = TransportMode::PLAIN;
        options.clusterGroupId = 9001;
        options.clusterGroupEpoch = 1;

        auto n1 = makeRuntime(dir / "n1", cfg, 1, options);
        auto n2 = makeRuntime(dir / "n2", cfg, 2, options);
        n1->runtime->start();
        n2->runtime->start();
        AKK_CLUSTER_CHECK(n1->runtime->role() == NodeRole::PRIMARY);
        AKK_CLUSTER_CHECK(n2->runtime->role() == NodeRole::PRIMARY);

        const std::string owner1Key = keyOwnedBy(cfg, 1, "partitioned-owner1");
        const std::string value = "owner-value";

        bool nonOwnerRejected = false;
        try {
            n2->runtime->shipEntry(1, ReplOpType::PUT, bytesOf(owner1Key), bytesOf(value), 0, 2);
        }
        catch (const std::runtime_error&) {
            nonOwnerRejected = true;
        }
        AKK_CLUSTER_CHECK(nonOwnerRejected);

        n1->runtime->shipEntry(1, ReplOpType::PUT, bytesOf(owner1Key), bytesOf(value), 0, 1);
        n1->setState(1, bytesOf(owner1Key), bytesOf(value));
        AKK_CLUSTER_CHECK(waitUntil([&] { return n2->hasState(1, owner1Key, value); }, std::chrono::milliseconds{8000}));

        // A partition owner consumes local storage sequences while applying
        // writes from other owners, so its own replicated sequence can have
        // gaps. The receiving peer must accept the forward gap and must force
        // the mutation durable before publishing peer progress.
        const std::string owner1KeyAfterGap = keyOwnedBy(cfg, 1, "partitioned-owner1-after-gap");
        const std::string valueAfterGap = "owner-value-after-gap";
        n1->runtime->shipEntry(3, ReplOpType::PUT, bytesOf(owner1KeyAfterGap), bytesOf(valueAfterGap), 0, 1);
        n1->setState(3, bytesOf(owner1KeyAfterGap), bytesOf(valueAfterGap));
        AKK_CLUSTER_CHECK(waitUntil(
            [&] { return n2->hasState(3, owner1KeyAfterGap, valueAfterGap); },
            std::chrono::milliseconds{8000}
        ));
        AKK_CLUSTER_CHECK(waitUntil([&] { return n2->durableCount.load() >= 2; }));

        n2->runtime->close();
        n1->runtime->close();
    }

    void testPartitionedEngineOwnerLinearizableRead() {
        const auto dir = makeTempDir("partitioned-engine-owner-read");
        const ClusterConfig cfg{
            {
                node(1, 21611, 21711),
                node(2, 21612, 21712),
            },
            ReplicationMode::PARTITIONED,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 500},
        };

        auto makeOptions = [&](uint64_t nodeId) {
            auto options = akkaradb::engine::AkkEngineOptions{};
            options.paths.dataDir = dir / ("n" + std::to_string(nodeId));
            options.components.clusterEnabled = true;
            options.components.blobEnabled = false;
            options.cluster.config = cfg;
            options.cluster.runtime.transportMode = TransportMode::PLAIN;
            options.cluster.runtime.clusterGroupId = 9002;
            options.cluster.runtime.clusterGroupEpoch = 1;
            options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
            writeNodeIdFile(options.paths.dataDir / "node.id", nodeId);
            return options;
        };

        auto n1 = akkaradb::engine::AkkEngine::open(makeOptions(1));
        auto n2 = akkaradb::engine::AkkEngine::open(makeOptions(2));

        const std::string owner1Key = keyOwnedBy(cfg, 1, "engine-owner1");
        const std::string value = "linearizable-owner-value";

        bool nonOwnerRejected = false;
        try {
            n2->put(bytesOf(owner1Key), bytesOf("wrong-owner"));
        }
        catch (const std::runtime_error&) {
            nonOwnerRejected = true;
        }
        AKK_CLUSTER_CHECK(nonOwnerRejected);

        n1->put(bytesOf(owner1Key), bytesOf(value));
        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                try {
                    const auto stored = engineGet(*n2, owner1Key);
                    return stored && *stored == std::vector<uint8_t>{value.begin(), value.end()};
                }
                catch (const std::runtime_error&) {
                    return false;
                }
            },
            std::chrono::milliseconds{8000}
        ));

        const std::string owner2Key = keyOwnedBy(cfg, 2, "engine-owner2");
        const std::string owner2Value = "owner2-interleaved-value";
        n2->put(bytesOf(owner2Key), bytesOf(owner2Value));
        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                const auto stored = engineGet(*n1, owner2Key);
                return stored && *stored == std::vector<uint8_t>{owner2Value.begin(), owner2Value.end()};
            },
            std::chrono::milliseconds{8000}
        ));

        const std::string updatedValue = "owner1-after-interleaved-remote-write";
        n1->put(bytesOf(owner1Key), bytesOf(updatedValue));
        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                const auto stored = engineGet(*n2, owner1Key);
                return stored && *stored == std::vector<uint8_t>{updatedValue.begin(), updatedValue.end()};
            },
            std::chrono::milliseconds{8000}
        ));

        n2->close();
        n1->close();
    }

    void testStripeEngineOwnerOnlyWritesAndCoordinatorReads() {
        const auto dir = makeTempDir("stripe-engine-owner-read");
        StripeOptions stripeOptions;
        stripeOptions.dataShards = 2;
        stripeOptions.parityShards = 1;
        const AckPolicy appliedAck{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::APPLIED};
        const ClusterConfig cfg{
            {
                node(1, 21621, 21721),
                node(2, 21622, 21722),
                node(3, 21623, 21723),
            },
            ReplicationMode::STRIPE,
            appliedAck,
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 750},
            {},
            stripeOptions,
        };

        auto makeOptions = [&](uint64_t nodeId, StripeReadCoordinatorMode coordinatorMode = StripeReadCoordinatorMode::OWNER) {
            auto options = akkaradb::engine::AkkEngineOptions{};
            options.paths.dataDir = dir / ("n" + std::to_string(nodeId));
            options.components.clusterEnabled = true;
            options.components.blobEnabled = false;
            options.cluster.config = cfg;
            options.cluster.runtime.transportMode = TransportMode::PLAIN;
            options.cluster.runtime.clusterGroupId = 9011;
            options.cluster.runtime.clusterGroupEpoch = 1;
            options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
            options.cluster.runtime.stripeReadCoordinatorMode = coordinatorMode;
            writeNodeIdFile(options.paths.dataDir / "node.id", nodeId);
            return options;
        };

        auto n1 = akkaradb::engine::AkkEngine::open(makeOptions(1));
        auto n2 = akkaradb::engine::AkkEngine::open(makeOptions(2, StripeReadCoordinatorMode::LOCAL_COORDINATOR));
        auto n3 = akkaradb::engine::AkkEngine::open(makeOptions(3));

        const std::string owner1Key = keyOwnedBy(cfg, 1, "stripe-owner1");
        const std::string value = "stripe-owner-value";

        bool nonOwnerRejected = false;
        try {
            n2->put(bytesOf(owner1Key), bytesOf("wrong-owner"));
        }
        catch (const std::runtime_error&) {
            nonOwnerRejected = true;
        }
        AKK_CLUSTER_CHECK(nonOwnerRejected);

        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                try {
                    n1->put(bytesOf(owner1Key), bytesOf(value));
                    return true;
                }
                catch (const std::runtime_error&) {
                    return false;
                }
            },
            std::chrono::milliseconds{8000}
        ));

        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                try {
                    const auto ownerRead = engineGet(*n3, owner1Key);
                    const auto localCoordinatorRead = engineGet(*n2, owner1Key);
                    const std::vector<uint8_t> expected{value.begin(), value.end()};
                    return ownerRead && localCoordinatorRead && *ownerRead == expected && *localCoordinatorRead == expected;
                }
                catch (const std::runtime_error&) {
                    return false;
                }
            },
            std::chrono::milliseconds{8000}
        ));

        n3->close();
        n2->close();
        n1->close();
    }

    void testStripeEngineRejectsDegradedWriteCommit() {
        const auto dir = makeTempDir("stripe-engine-data-shard-commit");
        StripeOptions stripeOptions;
        stripeOptions.dataShards = 2;
        stripeOptions.parityShards = 1;
        const AckPolicy appliedAck{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::APPLIED};
        const ClusterConfig cfg{
            {
                node(1, 21631, 21731),
                node(2, 21632, 21732),
                node(3, 21633, 21733),
            },
            ReplicationMode::STRIPE,
            appliedAck,
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 250},
            {},
            stripeOptions,
        };

        auto makeOptions = [&](uint64_t nodeId, StripeReadCoordinatorMode coordinatorMode) {
            auto options = akkaradb::engine::AkkEngineOptions{};
            options.paths.dataDir = dir / ("n" + std::to_string(nodeId));
            options.components.clusterEnabled = true;
            options.components.blobEnabled = false;
            options.cluster.config = cfg;
            options.cluster.runtime.transportMode = TransportMode::PLAIN;
            options.cluster.runtime.clusterGroupId = 9012;
            options.cluster.runtime.clusterGroupEpoch = 1;
            options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
            options.cluster.runtime.stripeWriteCommitMode = static_cast<StripeWriteCommitMode>(1);
            options.cluster.runtime.stripeReadCoordinatorMode = coordinatorMode;
            writeNodeIdFile(options.paths.dataDir / "node.id", nodeId);
            return options;
        };

        bool rejected = false;
        try {
            auto engine = akkaradb::engine::AkkEngine::open(makeOptions(1, StripeReadCoordinatorMode::OWNER));
            (void)engine;
        }
        catch (const std::invalid_argument&) {
            rejected = true;
        }
        AKK_CLUSTER_CHECK(rejected);
    }

    void testStripeEngineRejectsOnlineReconfiguration() {
        const auto dir = makeTempDir("stripe-engine-reconfigure");
        StripeOptions stripeOptions;
        stripeOptions.dataShards = 2;
        stripeOptions.parityShards = 1;
        const AckPolicy appliedAck{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::APPLIED};
        const ClusterConfig oldCfg{
            {
                node(1, 21641, 21741),
                node(2, 21642, 21742),
                node(3, 21643, 21743),
            },
            ReplicationMode::STRIPE,
            appliedAck,
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 2000},
            {},
            stripeOptions,
        };
        const ClusterConfig newCfg{
            {
                node(1, 21641, 21741),
                node(2, 21642, 21742),
                node(3, 21643, 21743),
                node(4, 21644, 21744),
            },
            ReplicationMode::STRIPE,
            appliedAck,
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 2000},
            {},
            stripeOptions,
        };

        auto makeOptions = [&](uint64_t nodeId, const ClusterConfig& cfg) {
            auto options = akkaradb::engine::AkkEngineOptions{};
            options.paths.dataDir = dir / ("n" + std::to_string(nodeId));
            options.components.clusterEnabled = true;
            options.components.blobEnabled = false;
            options.cluster.config = cfg;
            options.cluster.runtime.transportMode = TransportMode::PLAIN;
            options.cluster.runtime.clusterGroupId = 9013;
            options.cluster.runtime.clusterGroupEpoch = 1;
            options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
            options.cluster.runtime.stripeReadCoordinatorMode = StripeReadCoordinatorMode::LOCAL_COORDINATOR;
            writeNodeIdFile(options.paths.dataDir / "node.id", nodeId);
            return options;
        };

        auto n1 = akkaradb::engine::AkkEngine::open(makeOptions(1, oldCfg));
        auto n2 = akkaradb::engine::AkkEngine::open(makeOptions(2, oldCfg));
        auto n3 = akkaradb::engine::AkkEngine::open(makeOptions(3, oldCfg));

        bool rejected = false;
        try {
            n1->reconfigureCluster(newCfg);
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        AKK_CLUSTER_CHECK(rejected);
        n3->close();
        n2->close();
        n1->close();
    }

    void testClusterManagerRecoversPrimaryLeaseFromManifest() {
        const auto dir = makeTempDir("manifest-primary-lease");
        const ClusterConfig cfg{
            {
                node(1, 20321, 20421),
                node(2, 20322, 20422),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
        };

        {
            auto mf = manifest::Manifest::create(dir / "cluster.akmf", false);
            mf->primaryLease(1, nowUs() + 30'000'000);
            mf->nodeJoin(1, 25421, "manifest-primary.local");
            mf->close();
        }

        auto replica = ClusterManager::create(dir, cfg, 2);
        replica->start();
        AKK_CLUSTER_CHECK(replica->role() == NodeRole::REPLICA);
        AKK_CLUSTER_CHECK(replica->primaryNodeId() == 1);
        AKK_CLUSTER_CHECK(replica->primaryHost() == "manifest-primary.local");
        AKK_CLUSTER_CHECK(replica->primaryReplPort() == 25421);
        replica->close();
    }

    void testClusterManagerIgnoresExpiredManifestPrimaryLease() {
        const auto dir = makeTempDir("manifest-expired-primary-lease");
        const ClusterConfig cfg{
            {
                node(1, 20331, 20431),
                node(2, 20332, 20432),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
        };

        {
            auto mf = manifest::Manifest::create(dir / "cluster.akmf", false);
            mf->primaryLease(1, nowUs() - 1);
            mf->nodeJoin(1, 25431, "expired-primary.local");
            mf->close();
        }

        auto replica = ClusterManager::create(dir, cfg, 2);
        bool rejected = false;
        try {
            replica->start();
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        AKK_CLUSTER_CHECK(rejected);
    }

    void testClusterManagerRejectsAutoRoleAfterExpiredManifestLease() {
        const auto dir = makeTempDir("manifest-expired-auto-role");
        const ClusterConfig cfg{
            {
                node(1, 20341, 20441),
                node(2, 20342, 20442),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
        };

        {
            auto mf = manifest::Manifest::create(dir / "cluster.akmf", false);
            mf->primaryLease(2, nowUs() - 1);
            mf->nodeJoin(2, 25442, "expired-secondary.local");
            mf->close();
        }

        auto manager = ClusterManager::create(dir, cfg, 1);
        bool rejected = false;
        try {
            manager->start();
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        AKK_CLUSTER_CHECK(rejected);
    }

    void testClusterManagerRejectsPrimaryStartupWithForeignLease() {
        const auto dir = makeTempDir("manifest-foreign-primary-lease");
        const ClusterConfig cfg{
            {
                node(1, 20346, 20446),
                node(2, 20347, 20447),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
        };

        {
            auto mf = manifest::Manifest::create(dir / "cluster.akmf", false);
            mf->primaryLease(2, nowUs() + 30'000'000);
            mf->nodeJoin(2, 25447, "foreign-primary.local");
            mf->close();
        }

        ClusterRuntimeOptions options;
        options.startupRole = NodeStartupRole::PRIMARY;
        auto manager = ClusterManager::create(dir, cfg, 1, options);
        bool rejected = false;
        try {
            manager->start();
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        AKK_CLUSTER_CHECK(rejected);
    }

    void testClusterManagerDoesNotAutoPromoteRaftAfterExpiredManifestLease() {
        const auto dir = makeTempDir("manifest-expired-raft-lease");
        const ClusterConfig cfg{
            {
                node(1, 20351, 20451),
                node(2, 20352, 20452),
                node(3, 20353, 20453),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM},
        };

        {
            auto mf = manifest::Manifest::create(dir / "cluster.akmf", false);
            mf->primaryLease(2, nowUs() - 1);
            mf->close();
        }

        auto primary = ClusterManager::create(dir, cfg, 1);
        bool rejected = false;
        try {
            primary->start();
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        AKK_CLUSTER_CHECK(rejected);
    }

    void testClusterManagerReportsManifestActiveNodes() {
        const auto dir = makeTempDir("manifest-active-nodes");
        const ClusterConfig cfg{
            {
                node(1, 20361, 20461),
                node(2, 20362, 20462),
                node(3, 20363, 20463),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
        };

        {
            auto mf = manifest::Manifest::create(dir / "cluster.akmf", false);
            mf->nodeJoin(1, 25461, "active-one.local");
            mf->nodeJoin(2, 25462, "left-two.local");
            mf->nodeLeave(2);
            mf->nodeJoin(3, 25463, "active-three.local");
            mf->nodeJoin(99, 25499, "ignored-config-external.local");
            mf->close();
        }

        auto manager = ClusterManager::create(dir, cfg, 1);
        const auto active = manager->activeNodes();
        AKK_CLUSTER_CHECK(active.size() == 2);
        AKK_CLUSTER_CHECK(active[0].nodeId == 1);
        AKK_CLUSTER_CHECK(active[0].host == "active-one.local");
        AKK_CLUSTER_CHECK(active[0].dataPort == 20361);
        AKK_CLUSTER_CHECK(active[0].replPort == 25461);
        AKK_CLUSTER_CHECK(active[1].nodeId == 3);
        AKK_CLUSTER_CHECK(active[1].host == "active-three.local");
        AKK_CLUSTER_CHECK(active[1].dataPort == 20363);
        AKK_CLUSTER_CHECK(active[1].replPort == 25463);
    }

    void testClusterManagerContainsLeaseRenewalFailure() {
        ScopedLeaseRenewalFailure injectedFailure;
        const auto dir = makeTempDir("lease-renewal-failure");
        const ClusterConfig cfg{
            {
                node(1, 20371, 20471),
                node(2, 20372, 20472),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
        };
        ClusterRuntimeOptions options;
        options.startupRole = NodeStartupRole::PRIMARY;
        auto manager = ClusterManager::create(dir, cfg, 1, options);
        manager->start();
        AKK_CLUSTER_CHECK(manager->role() == NodeRole::PRIMARY);
        AKK_CLUSTER_CHECK(waitUntil(
            [&] { return manager->role() == NodeRole::REPLICA; },
            std::chrono::milliseconds{2000}
        ));

        bool reported = false;
        try { manager->checkHealth(); }
        catch (const std::runtime_error& error) {
            reported = std::string_view{error.what()}.find("injected lease renewal failure") != std::string_view::npos;
        }
        AKK_CLUSTER_CHECK(reported);
        manager->close();
    }

    void testClusterRuntimeReportsLeaseRenewalFailure() {
        ScopedLeaseRenewalFailure injectedFailure;
        const auto dir = makeTempDir("runtime-lease-renewal-stats");
        const ClusterConfig cfg{
            {node(1, 23601, 23611), node(2, 23602, 23612)},
            ReplicationMode::MIRROR,
            AckPolicy{},
        };
        ClusterRuntimeOptions options;
        options.transportMode = TransportMode::PLAIN;
        options.startupRole = NodeStartupRole::PRIMARY;
        auto runtime = makeRuntime(dir / "n1", cfg, 1, options);
        runtime->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                return runtime->runtime->role() == NodeRole::REPLICA &&
                       runtime->runtime->raftStats().leaseRenewFailures == 1;
            },
            std::chrono::milliseconds{2000}
        ));
        const auto stats = runtime->runtime->raftStats();
        AKK_CLUSTER_CHECK(stats.health == akkaradb::engine::ClusterHealthState::FAILED);
        AKK_CLUSTER_CHECK(stats.lastFailure == akkaradb::engine::ClusterFailureCode::LEASE_RENEWAL);
        AKK_CLUSTER_CHECK(stats.lastFailureAtUs != 0 && stats.leaseRenewFailures == 1);
        runtime->runtime->close();
    }

    void testStripeErasureCodecRecovery() {
        const std::string value = "AkkaraDB parity stripe recovery across missing shards";
        const erasure::ErasureLayout layout{.dataShards = 4, .parityShards = 2};
        auto shards = erasure::RsErasureCodec::encode(bytesOf(value), layout);
        AKK_CLUSTER_CHECK(shards.size() == 6);

        for (size_t a = 0; a < shards.size(); ++a) {
            for (size_t b = a + 1; b < shards.size(); ++b) {
                for (size_t c = b + 1; c < shards.size(); ++c) {
                    for (size_t d = c + 1; d < shards.size(); ++d) {
                        std::vector<erasure::ErasureShard> available{shards[a], shards[b], shards[c], shards[d]};
                        const auto recovered = erasure::RsErasureCodec::decode(available, layout);
                        AKK_CLUSTER_CHECK(textOf(recovered) == value);
                    }
                }
            }
        }

        for (uint16_t missingIndex = 0; missingIndex < layout.totalShards(); ++missingIndex) {
            std::vector<erasure::ErasureShard> availableForRepair;
            availableForRepair.reserve(shards.size() - 1);
            for (const auto& shard : shards) {
                if (shard.index != missingIndex) { availableForRepair.push_back(shard); }
            }
            const auto repairedShard = erasure::RsErasureCodec::repairOne(missingIndex, availableForRepair, layout);
            AKK_CLUSTER_CHECK(repairedShard.index == missingIndex);
            AKK_CLUSTER_CHECK(repairedShard.originalSize == shards[missingIndex].originalSize);
            AKK_CLUSTER_CHECK(repairedShard.payload == shards[missingIndex].payload);
            AKK_CLUSTER_CHECK(repairedShard.crc32c == shards[missingIndex].crc32c);
        }
        const auto repairedParityWithMissingData =
            erasure::RsErasureCodec::repairOne(4, std::vector<erasure::ErasureShard>{shards[0], shards[2], shards[3], shards[5]}, layout);
        AKK_CLUSTER_CHECK(repairedParityWithMissingData.index == 4);
        AKK_CLUSTER_CHECK(repairedParityWithMissingData.payload == shards[4].payload);
        AKK_CLUSTER_CHECK(repairedParityWithMissingData.crc32c == shards[4].crc32c);

        const std::string cauchyValue = "AkkaraDB Cauchy RS matrix regression";
        const erasure::ErasureLayout cauchyLayout{.dataShards = 3, .parityShards = 4};
        const auto cauchyShards = erasure::RsErasureCodec::encode(bytesOf(cauchyValue), cauchyLayout);
        for (size_t a = 0; a < cauchyShards.size(); ++a) {
            for (size_t b = a + 1; b < cauchyShards.size(); ++b) {
                for (size_t c = b + 1; c < cauchyShards.size(); ++c) {
                    std::vector<erasure::ErasureShard> available{cauchyShards[a], cauchyShards[b], cauchyShards[c]};
                    const auto recovered = erasure::RsErasureCodec::decode(available, cauchyLayout);
                    AKK_CLUSTER_CHECK(textOf(recovered) == cauchyValue);
                }
            }
        }

        const auto assertBoundaryRoundTrip = [](const std::vector<uint8_t>& payload) {
            const erasure::ErasureLayout boundaryLayout{.dataShards = 4, .parityShards = 2};
            const auto encoded = erasure::RsErasureCodec::encode(std::span<const uint8_t>{payload.data(), payload.size()}, boundaryLayout);
            AKK_CLUSTER_CHECK(encoded.size() == boundaryLayout.totalShards());
            const auto decoded = erasure::RsErasureCodec::decode(
                std::vector<erasure::ErasureShard>{encoded[0], encoded[2], encoded[3], encoded[5]},
                boundaryLayout
            );
            AKK_CLUSTER_CHECK(decoded == payload);
        };
        assertBoundaryRoundTrip({});
        assertBoundaryRoundTrip({0xA5});
        assertBoundaryRoundTrip({0, 1, 2, 3, 4, 5, 6, 7});
        assertBoundaryRoundTrip({0, 1, 2, 3, 4, 5, 6, 7, 8});
        AKK_CLUSTER_CHECK(erasure::RsErasureCodec::shardPayloadSize(std::numeric_limits<uint64_t>::max(), 2) ==
                          (std::numeric_limits<uint64_t>::max() / 2u) + 1u);

        const auto repaired = erasure::RsErasureCodec::repairOne(1, std::vector<erasure::ErasureShard>{shards[0], shards[2], shards[3], shards[4]}, layout);
        AKK_CLUSTER_CHECK(repaired.index == 1);
        AKK_CLUSTER_CHECK(repaired.payload == shards[1].payload);
        AKK_CLUSTER_CHECK(repaired.crc32c == shards[1].crc32c);

        auto corrupted = shards;
        corrupted[0].payload[0] ^= 0x7Fu;
        bool crcRejected = false;
        try {
            (void)erasure::RsErasureCodec::decode(std::vector<erasure::ErasureShard>{corrupted[0], corrupted[1], corrupted[2], corrupted[3]}, layout);
        }
        catch (const std::runtime_error&) {
            crcRejected = true;
        }
        AKK_CLUSTER_CHECK(crcRejected);

        std::vector<erasure::ErasureShard> available{shards[0], shards[2], shards[4]};
        bool rejected = false;
        try {
            (void)erasure::RsErasureCodec::decode(available, layout);
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        AKK_CLUSTER_CHECK(rejected);
    }

    void testRsErasureCodecProperties() {
        const auto runDecodeSample = [](erasure::ErasureLayout layout, size_t payloadSize, uint64_t seed) {
            const auto payload = deterministicBytes(payloadSize, seed);
            const auto shards = erasure::RsErasureCodec::encode(std::span<const uint8_t>{payload.data(), payload.size()}, layout);
            const auto selected = selectShards(shards, layout, seed ^ 0x9E3779B97F4A7C15ull);
            const auto decoded = erasure::RsErasureCodec::decode(selected, layout);
            AKK_CLUSTER_CHECK(decoded == payload);
        };

        const auto runFullCombinationDecode = [](erasure::ErasureLayout layout, size_t payloadSize, uint64_t seed) {
            const auto payload = deterministicBytes(payloadSize, seed);
            const auto shards = erasure::RsErasureCodec::encode(std::span<const uint8_t>{payload.data(), payload.size()}, layout);

            std::vector<uint16_t> picked;
            picked.reserve(layout.dataShards);
            const auto walk = [&](auto&& self, uint16_t offset) -> void {
                if (picked.size() == layout.dataShards) {
                    std::vector<erasure::ErasureShard> selected;
                    selected.reserve(layout.dataShards);
                    for (const auto index : picked) { selected.push_back(shards[index]); }
                    const auto decoded = erasure::RsErasureCodec::decode(selected, layout);
                    AKK_CLUSTER_CHECK(decoded == payload);
                    return;
                }

                const auto remaining = static_cast<uint16_t>(layout.dataShards - picked.size());
                for (uint16_t i = offset; i + remaining <= layout.totalShards(); ++i) {
                    picked.push_back(i);
                    self(self, static_cast<uint16_t>(i + 1u));
                    picked.pop_back();
                }
            };
            walk(walk, 0);
        };

        runFullCombinationDecode({.dataShards = 2, .parityShards = 1}, 17, 0x2001);
        runFullCombinationDecode({.dataShards = 2, .parityShards = 3}, 31, 0x2002);
        runFullCombinationDecode({.dataShards = 3, .parityShards = 4}, 43, 0x2003);
        runFullCombinationDecode({.dataShards = 4, .parityShards = 4}, 71, 0x2004);
        runFullCombinationDecode({.dataShards = 6, .parityShards = 3}, 97, 0x2005);

        const std::array sampledLayouts{
            erasure::ErasureLayout{.dataShards = 8, .parityShards = 4},
            erasure::ErasureLayout{.dataShards = 10, .parityShards = 6},
            erasure::ErasureLayout{.dataShards = 16, .parityShards = 8},
            erasure::ErasureLayout{.dataShards = 32, .parityShards = 8},
            erasure::ErasureLayout{.dataShards = 64, .parityShards = 16},
        };

        for (size_t layoutIndex = 0; layoutIndex < sampledLayouts.size(); ++layoutIndex) {
            const auto sampledLayout = sampledLayouts[layoutIndex];
            for (uint64_t sample = 0; sample < 32; ++sample) {
                runDecodeSample(sampledLayout, 128 + layoutIndex * 31 + static_cast<size_t>(sample * 7), 0x5000 + layoutIndex * 257 + sample);
            }

            const auto payload = deterministicBytes(256 + layoutIndex * 17, 0x7000 + layoutIndex);
            const auto shards = erasure::RsErasureCodec::encode(std::span<const uint8_t>{payload.data(), payload.size()}, sampledLayout);
            for (const uint16_t missingIndex : {
                     uint16_t{0},
                     static_cast<uint16_t>(sampledLayout.dataShards - 1u),
                     sampledLayout.dataShards,
                     static_cast<uint16_t>(sampledLayout.totalShards() - 1u),
                 }) {
                std::vector<erasure::ErasureShard> available;
                available.reserve(shards.size() - 1);
                for (const auto& shard : shards) {
                    if (shard.index != missingIndex) { available.push_back(shard); }
                }
                const auto repaired = erasure::RsErasureCodec::repairOne(missingIndex, available, sampledLayout);
                AKK_CLUSTER_CHECK(repaired.index == missingIndex);
                AKK_CLUSTER_CHECK(repaired.payload == shards[missingIndex].payload);
                AKK_CLUSTER_CHECK(repaired.crc32c == shards[missingIndex].crc32c);
            }
        }
    }

    void testErsCodecRecovery() {
        const auto containsIndex = [](const std::vector<uint16_t>& indices, uint16_t expected) {
            return std::find(indices.begin(), indices.end(), expected) != indices.end();
        };
        const auto containsShard = [](const std::vector<erasure::ErasureShard>& shards, const erasure::ErasureShard& expected) {
            return std::find_if(
                       shards.begin(),
                       shards.end(),
                       [&expected](const erasure::ErasureShard& shard) {
                           return shard.index == expected.index &&
                                  shard.originalSize == expected.originalSize &&
                                  shard.payload == expected.payload &&
                                  shard.crc32c == expected.crc32c;
                       }
                   ) != shards.end();
        };

        const std::string value = "AkkaraDB ERS corruption and erasure recovery";
        const erasure::ErasureLayout layout{.dataShards = 4, .parityShards = 3};
        const auto shards = erasure::ErsCodec::encode(bytesOf(value), layout);

        {
            std::vector<erasure::ErasureShard> missing{shards[0], shards[1], shards[3], shards[4], shards[6]};
            const auto recovered = erasure::ErsCodec::recover(missing, layout);
            AKK_CLUSTER_CHECK(textOf(recovered.value) == value);
            AKK_CLUSTER_CHECK(recovered.missingIndices.size() == 2);
            AKK_CLUSTER_CHECK(containsIndex(recovered.missingIndices, 2));
            AKK_CLUSTER_CHECK(containsIndex(recovered.missingIndices, 5));
            AKK_CLUSTER_CHECK(recovered.detectedErrorIndices.empty());
            AKK_CLUSTER_CHECK(containsShard(recovered.repairedShards, shards[2]));
            AKK_CLUSTER_CHECK(containsShard(recovered.repairedShards, shards[5]));
        }

        {
            auto corrupted = shards;
            corrupted[1].payload[0] ^= 0x33u;
            const auto recovered = erasure::ErsCodec::recover(corrupted, layout);
            AKK_CLUSTER_CHECK(textOf(recovered.value) == value);
            AKK_CLUSTER_CHECK(recovered.missingIndices.empty());
            AKK_CLUSTER_CHECK(recovered.detectedErrorIndices.size() == 1);
            AKK_CLUSTER_CHECK(recovered.detectedErrorIndices[0] == 1);
            AKK_CLUSTER_CHECK(containsShard(recovered.repairedShards, shards[1]));
        }

        {
            auto suspicious = shards;
            suspicious[4].payload[0] ^= 0x55u;
            const std::array<uint16_t, 1> knownBad{4};
            const auto recovered = erasure::ErsCodec::recover(suspicious, layout, knownBad);
            AKK_CLUSTER_CHECK(textOf(recovered.value) == value);
            AKK_CLUSTER_CHECK(recovered.missingIndices.empty());
            AKK_CLUSTER_CHECK(recovered.detectedErrorIndices.size() == 1);
            AKK_CLUSTER_CHECK(recovered.detectedErrorIndices[0] == 4);
            AKK_CLUSTER_CHECK(containsShard(recovered.repairedShards, shards[4]));
        }

        {
            auto corrupted = shards;
            corrupted[0].payload[0] ^= 0x11u;
            corrupted[5].payload[0] ^= 0x22u;
            const auto recovered = erasure::ErsCodec::recover(corrupted, layout);
            AKK_CLUSTER_CHECK(textOf(recovered.value) == value);
            AKK_CLUSTER_CHECK(recovered.detectedErrorIndices.size() == 2);
            AKK_CLUSTER_CHECK(containsIndex(recovered.detectedErrorIndices, 0));
            AKK_CLUSTER_CHECK(containsIndex(recovered.detectedErrorIndices, 5));
            AKK_CLUSTER_CHECK(containsShard(recovered.repairedShards, shards[0]));
            AKK_CLUSTER_CHECK(containsShard(recovered.repairedShards, shards[5]));
        }

        {
            std::vector<erasure::ErasureShard> insufficient{shards[0], shards[2], shards[4], shards[6]};
            bool rejected = false;
            try {
                const std::array<uint16_t, 1> knownBad{0};
                (void)erasure::ErsCodec::recover(insufficient, layout, knownBad);
            }
            catch (const std::runtime_error&) {
                rejected = true;
            }
            AKK_CLUSTER_CHECK(rejected);
        }

        {
            bool rejected = false;
            try {
                const std::array<uint16_t, 1> knownBad{layout.totalShards()};
                (void)erasure::ErsCodec::recover(shards, layout, knownBad);
            }
            catch (const std::invalid_argument&) {
                rejected = true;
            }
            AKK_CLUSTER_CHECK(rejected);
        }
    }

    void testRaftElectionAndLoopbackReplication() {
        const auto dir = makeTempDir("raft-loopback");
        const ClusterConfig cfg{
            {
                node(1, 20101, 20201),
                node(2, 20102, 20202),
                node(3, 20103, 20203),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 1000},
        };

        auto n1 = makeRuntime(dir / "n1", cfg, 1);
        auto n2 = makeRuntime(dir / "n2", cfg, 2);
        auto n3 = makeRuntime(dir / "n3", cfg, 3);

        n1->runtime->start();
        n2->runtime->start();
        n3->runtime->start();

        std::array<RuntimeHarness*, 3> nodes{n1.get(), n2.get(), n3.get()};
        ensurePreferredLeader(nodes, std::chrono::milliseconds{20000});

        const std::string key = "raft-key";
        const std::string value = "raft-value";
        RuntimeHarness* leader = nullptr;
        runOnLeader(
            nodes,
            [&](RuntimeHarness& currentLeader) {
                leader = &currentLeader;
                currentLeader.runtime->shipEntry(
                    1,
                    ReplOpType::PUT,
                    bytesOf(key),
                    bytesOf(value),
                    akkaradb::core::MemHdr16::FLAG_NORMAL,
                    currentLeader.nodeId
                );
            },
            std::chrono::milliseconds{8000}
        );
        AKK_CLUSTER_CHECK(leader != nullptr);

        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                size_t followersApplied = 0;
                for (const auto* n : nodes) {
                    if (n != leader && n->hasState(1, key, value)) { ++followersApplied; }
                }
                return followersApplied == 2;
            }
        ));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto stats = leader->runtime->raftStats();
            return stats.health == akkaradb::engine::ClusterHealthState::HEALTHY && stats.peers.size() == 2 &&
                   std::ranges::all_of(stats.peers, [](const RaftPeerStats& peer) {
                       return peer.connected && peer.lastSuccessfulContactAtUs != 0 &&
                              peer.roundTripsSucceeded != 0 && peer.lastRoundTripAtUs != 0 &&
                              peer.roundTripLatencyUs.sampleCount == peer.roundTripsSucceeded &&
                              peer.roundTripLatencyUs.bucketCounts.back() == peer.roundTripLatencyUs.sampleCount &&
                              std::ranges::is_sorted(peer.roundTripLatencyUs.bucketCounts);
                   });
        }));

        RuntimeHarness* follower = leader == n1.get() ? n2.get() : n1.get();
        bool rejected = false;
        try {
            follower->runtime->shipEntry(2, ReplOpType::PUT, bytesOf(key), bytesOf(value), 0, follower->nodeId);
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        AKK_CLUSTER_CHECK(rejected);

        n3->runtime->close();
        n2->runtime->close();
        n1->runtime->close();
    }

    void testRaftPipelinedProposalsAndReadIndex(bool secure = false) {
        const auto dir = makeTempDir(secure ? "raft-pipeline-secure" : "raft-pipeline-read-index");
        const ClusterConfig config{
            {node(1, 21821, 21921), node(2, 21822, 21922), node(3, 21823, 21923)},
            ReplicationMode::MIRROR, AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 2000},
        };
        ClusterRuntimeOptions options;
        options.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        options.raftSnapshot.minLogEntries = 128;
        if (secure) {
            for (uint64_t id = 1; id <= 3; ++id) {
                const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
                options.secure.pinnedPeers.push_back({id, identity.publicKey});
            }
        }
        auto makePeer = [&](uint64_t id) {
            auto peerOptions = options;
            peerOptions.secure.identitySeedPath = dir / ("identity-" + std::to_string(id));
            return makeRuntime(dir / ("n" + std::to_string(id)), config, id, peerOptions, secure);
        };
        auto n1 = makePeer(1);
        auto n2 = makePeer(2);
        auto n3 = makePeer(3);
        std::array<RuntimeHarness*, 3> nodes{n1.get(), n2.get(), n3.get()};
        for (auto* n : nodes) { n->runtime->start(); }
        ensurePreferredLeader(nodes);
        auto* leader = findLeader(nodes);
        AKK_CLUSTER_CHECK(leader != nullptr);
        std::vector<std::future<void>> pending;
        const auto before = leader->runtime->raftStats();
        for (uint64_t seq = 1; seq <= 128; ++seq) {
            pending.push_back(leader->runtime->submitEntry(seq, ReplOpType::PUT, bytesOf("pipeline"), bytesOf(std::to_string(seq)), 0, leader->nodeId));
        }
        for (auto& result : pending) { result.get(); }
        AKK_CLUSTER_CHECK(leader->appliedCount.load() == 128);
        AKK_CLUSTER_CHECK(leader->runtime->raftStats().proposalBatches - before.proposalBatches < 128);
        AKK_CLUSTER_CHECK(waitUntil([&] {
            return std::ranges::all_of(nodes, [](auto* n) { return n->hasState(128, "pipeline", "128"); });
        }));
        const auto stable = leader->runtime->raftStats();
        const auto metadataPath = dir / ("n" + std::to_string(leader->nodeId)) / "cluster-raft.log";
        AKK_CLUSTER_CHECK(waitUntil([&] { return readRaftLogLastIncludedIndex(metadataPath) >= 128; }));
        const auto metadata = readTestFile(metadataPath);
        const auto modified = std::filesystem::last_write_time(metadataPath);
        std::vector<std::future<ReadResponse>> reads;
        for (unsigned i = 0; i < 24; ++i) {
            reads.push_back(std::async(std::launch::async, [&] { return leader->runtime->readKey(bytesOf("pipeline"), 0); }));
        }
        for (auto& read : reads) {
            const auto response = read.get();
            AKK_CLUSTER_CHECK(response.status == ReadStatus::FOUND && textOf(response.value) == "128");
        }
        AKK_CLUSTER_CHECK(readTestFile(metadataPath) == metadata);
        AKK_CLUSTER_CHECK(std::filesystem::last_write_time(metadataPath) == modified);
        const auto afterReads = leader->runtime->raftStats();
        AKK_CLUSTER_CHECK(afterReads.outboundConnections == stable.outboundConnections);
        AKK_CLUSTER_CHECK(afterReads.peerWorkers == stable.peerWorkers);
        // One lost peer does not block majority progress; reconnect restores it.
        RuntimeHarness* stopped = leader == n3.get() ? n2.get() : n3.get();
        stopped->runtime->close();
        leader->runtime->shipEntry(129, ReplOpType::PUT, bytesOf("pipeline"), bytesOf("129"), 0, leader->nodeId);
        stopped->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return stopped->hasState(129, "pipeline", "129"); }));
        AKK_CLUSTER_CHECK(leader->runtime->readKey(bytesOf("pipeline"), 0).status == ReadStatus::FOUND);
        // Prior successful ACKs must not authorize reads after quorum loss.
        for (auto* n : nodes) { if (n != leader) { n->runtime->close(); } }
        AKK_CLUSTER_CHECK(leader->runtime->readKey(bytesOf("pipeline"), 0).status == ReadStatus::ERROR_STATUS);
        auto timedOut = leader->runtime->submitEntry(130, ReplOpType::PUT, bytesOf("pipeline"), bytesOf("uncommitted"), 0, leader->nodeId);
        bool rejected = false;
        try { timedOut.get(); } catch (const std::exception&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected && leader->hasState(129, "pipeline", "129"));
        auto interrupted = leader->runtime->submitEntry(131, ReplOpType::PUT, bytesOf("pipeline"), bytesOf("closing"), 0, leader->nodeId);
        leader->runtime->close();
        AKK_CLUSTER_CHECK(interrupted.wait_for(std::chrono::seconds{1}) == std::future_status::ready);
        rejected = false;
        try { interrupted.get(); } catch (const std::exception&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected);
    }

    void testChunkedTransferValidation() {
        namespace transfer = akkaradb::engine::cluster::detail;
        const auto dir = makeTempDir("chunked-transfer-validation");
        ReplicationTransferOptions options;
        options.thresholdBytes = 128;
        options.chunkBytes = 1024;
        options.maxConcurrentTransfers = 1;
        options.maxMemoryBytes = 512 * 1024;
        options.maxSpoolBytes = 4 * 1024 * 1024;
        options.spoolDirectory = dir;
        auto sender = std::make_shared<transfer::TransferBudget>(options);
        auto receiver = std::make_shared<transfer::TransferBudget>(options);
        const std::string value(512 * 1024, 's');
        auto message = transfer::entryMessage(1, 1, ReplOpType::PUT, 0, bytesOf("key"), bytesOf(value), sender);
        AKK_CLUSTER_CHECK(sender->used(transfer::TransferBudget::Resource::SPOOL) == value.size() + 29);
        std::vector<std::vector<uint8_t>> frames;
        AKK_CLUSTER_CHECK(transfer::sendMessage(*message, sender, [&](std::span<const uint8_t> frame) {
            AKK_CLUSTER_CHECK(frame.size() <= transfer::TRANSFER_FRAME_LIMIT + ReplFrameHeader::SIZE);
            frames.emplace_back(frame.begin(), frame.end());
            return true;
        }));
        AKK_CLUSTER_CHECK(frames.size() > 500);
        auto receive = [&](const auto& input) {
            size_t index = 0;
            return transfer::receiveMessage(receiver, [&](DecodedFrame& out) {
                return index < input.size() && decodeFrame(input[index++], out);
            });
        };
        auto received = receive(frames);
        AKK_CLUSTER_CHECK(received);
        transfer::EntryView view;
        AKK_CLUSTER_CHECK(transfer::entryView(received->payload, view) && textOf(view.value) == value);
        received.reset();
        AKK_CLUSTER_CHECK(receiver->used(transfer::TransferBudget::Resource::SPOOL) == 0);
        auto incomplete = frames;
        incomplete.pop_back();
        AKK_CLUSTER_CHECK(!receive(incomplete));
        auto dropped = frames;
        dropped.erase(dropped.begin() + static_cast<std::ptrdiff_t>(dropped.size() / 2));
        AKK_CLUSTER_CHECK(!receive(dropped));
        auto duplicated = frames;
        duplicated.insert(duplicated.begin() + 2, duplicated[1]);
        AKK_CLUSTER_CHECK(!receive(duplicated));
        auto partial = frames;
        partial[partial.size() / 2].pop_back();
        AKK_CLUSTER_CHECK(!receive(partial));
        auto corrupt = frames;
        DecodedFrame changed;
        AKK_CLUSTER_CHECK(decodeFrame(corrupt[1], changed));
        changed.payload.back() ^= 1;
        corrupt[1] = encodeFrame(changed.type, changed.payload);
        AKK_CLUSTER_CHECK(!receive(corrupt)); // Per-chunk CRC passes; whole-transfer CRC must still fail.
        auto outOfOrder = frames;
        std::swap(outOfOrder[1], outOfOrder[2]);
        AKK_CLUSTER_CHECK(!receive(outOfOrder));
        size_t delayedIndex = 0;
        auto delayed = transfer::receiveMessage(receiver, [&](DecodedFrame& out) {
            if (delayedIndex >= frames.size()) { return false; }
            std::this_thread::sleep_for(std::chrono::microseconds{100});
            return decodeFrame(frames[delayedIndex++], out);
        });
        AKK_CLUSTER_CHECK(delayed && transfer::entryView(delayed->payload, view) && textOf(view.value) == value);
        delayed.reset();
        // The compatibility helper remains connection-local.
        received = receive(frames);
        AKK_CLUSTER_CHECK(received && transfer::entryView(received->payload, view) && textOf(view.value) == value);
        received.reset();
        AKK_CLUSTER_CHECK(receiver->used(transfer::TransferBudget::Resource::SPOOL) == 0);
        AKK_CLUSTER_CHECK(receiver->used(transfer::TransferBudget::Resource::ACTIVE) == 0);
        {
            auto occupied = sender->reserve(transfer::TransferBudget::Resource::ACTIVE, 1);
            bool rejected = false;
            try { (void)transfer::sendMessage(*message, sender, [](auto) { return true; }); }
            catch (const std::runtime_error&) { rejected = true; }
            AKK_CLUSTER_CHECK(rejected);
        }
        AKK_CLUSTER_CHECK(!transfer::sendMessage(*message, sender, [](auto) { return false; }));

        // Runtime sessions durably retain the validated prefix. Reconstructing
        // the budget simulates a process restart using the same spool directory.
        const size_t cut = frames.size() / 2;
        std::vector<std::vector<uint8_t>> firstAttempt(frames.begin(), frames.begin() + static_cast<std::ptrdiff_t>(cut));
        auto session = std::make_shared<transfer::TransferSession>();
        std::vector<std::vector<uint8_t>> readyFrames;
        size_t firstIndex = 0;
        AKK_CLUSTER_CHECK(!transfer::receiveMessage(receiver, session, [&](DecodedFrame& out) {
            return firstIndex < firstAttempt.size() && decodeFrame(firstAttempt[firstIndex++], out);
        }, [&](std::span<const uint8_t> wire) {
            readyFrames.emplace_back(wire.begin(), wire.end());
            return true;
        }));
        AKK_CLUSTER_CHECK(readyFrames.size() == 1);
        DecodedFrame ready;
        AKK_CLUSTER_CHECK(decodeFrame(readyFrames.front(), ready) && ready.type == ReplMsgType::TRANSFER_READY);
        AKK_CLUSTER_CHECK(readU64Le(ready.payload, 32) == 0);
        AKK_CLUSTER_CHECK(receiver->used(transfer::TransferBudget::Resource::SPOOL) > 0);
        receiver.reset();

        std::vector<std::vector<uint8_t>> secondAttempt;
        secondAttempt.push_back(frames.front());
        secondAttempt.insert(secondAttempt.end(), frames.begin() + static_cast<std::ptrdiff_t>(cut), frames.end());
        receiver = std::make_shared<transfer::TransferBudget>(options);
        session = std::make_shared<transfer::TransferSession>();
        readyFrames.clear();
        size_t secondIndex = 0;
        received = transfer::receiveMessage(receiver, session, [&](DecodedFrame& out) {
            return secondIndex < secondAttempt.size() && decodeFrame(secondAttempt[secondIndex++], out);
        }, [&](std::span<const uint8_t> wire) {
            readyFrames.emplace_back(wire.begin(), wire.end());
            return true;
        });
        AKK_CLUSTER_CHECK(received && transfer::entryView(received->payload, view) && textOf(view.value) == value);
        AKK_CLUSTER_CHECK(readyFrames.size() == 1 && decodeFrame(readyFrames.front(), ready));
        const auto resumedOffset = readU64Le(ready.payload, 32);
        AKK_CLUSTER_CHECK(resumedOffset > 0 && resumedOffset < received->payload.size());
        const auto resumeStats = receiver->stats();
        AKK_CLUSTER_CHECK(resumeStats.resumedTransfers == 1 && resumeStats.resumedBytes == resumedOffset);

        auto senderSession = std::make_shared<transfer::TransferSession>();
        std::mutex sentMutex;
        std::vector<std::vector<uint8_t>> resumedSendFrames;
        auto sendResult = std::async(std::launch::async, [&] {
            return transfer::sendMessage(*message, sender, senderSession, [&](std::span<const uint8_t> wire) {
                std::lock_guard lock{sentMutex};
                resumedSendFrames.emplace_back(wire.begin(), wire.end());
                return true;
            });
        });
        AKK_CLUSTER_CHECK(waitUntil([&] { std::lock_guard lock{sentMutex}; return !resumedSendFrames.empty(); }));
        DecodedFrame resumeBegin;
        {
            std::lock_guard lock{sentMutex};
            AKK_CLUSTER_CHECK(decodeFrame(resumedSendFrames.front(), resumeBegin));
        }
        transfer::TransferId transferId{};
        std::copy_n(resumeBegin.payload.begin() + 14, transferId.size(), transferId.begin());
        AKK_CLUSTER_CHECK(senderSession->notifyReady(transferId, resumedOffset));
        AKK_CLUSTER_CHECK(sendResult.get());
        DecodedFrame firstResumedChunk;
        {
            std::lock_guard lock{sentMutex};
            AKK_CLUSTER_CHECK(resumedSendFrames.size() > 2 && decodeFrame(resumedSendFrames[1], firstResumedChunk));
        }
        AKK_CLUSTER_CHECK(firstResumedChunk.type == ReplMsgType::TRANSFER_CHUNK &&
            readU64Le(firstResumedChunk.payload, 0) == resumedOffset);
        received.reset();
        AKK_CLUSTER_CHECK(receiver->used(transfer::TransferBudget::Resource::SPOOL) == 0);

        const auto staleDirectory = dir / "akkaradb-transfer-resume-v1" / "stale";
        std::filesystem::create_directories(staleDirectory);
        writeTextFile(staleDirectory / "expired.part", "stale");
        std::filesystem::last_write_time(staleDirectory / "expired.part",
            std::filesystem::file_time_type::clock::now() - std::chrono::hours{1});
        auto cleanupOptions = options;
        cleanupOptions.resumeRetentionMs = 1;
        auto cleanupBudget = std::make_shared<transfer::TransferBudget>(cleanupOptions);
        AKK_CLUSTER_CHECK(!std::filesystem::exists(staleDirectory / "expired.part"));
        AKK_CLUSTER_CHECK(cleanupBudget->stats().discardedPartials == 1);
        const auto capDirectory = dir / "akkaradb-transfer-resume-v1" / "capacity";
        std::filesystem::create_directories(capDirectory);
        const auto now = std::filesystem::file_time_type::clock::now();
        for (unsigned i = 0; i < 3; ++i) {
            const auto path = capDirectory / (std::to_string(i) + ".part");
            writeTextFile(path, "partial");
            std::filesystem::last_write_time(path, now - std::chrono::minutes{static_cast<int64_t>(3 - i)});
        }
        auto capOptions = options;
        capOptions.maxResumeTransfers = 2;
        auto capBudget = std::make_shared<transfer::TransferBudget>(capOptions);
        AKK_CLUSTER_CHECK(!std::filesystem::exists(capDirectory / "0.part"));
        AKK_CLUSTER_CHECK(std::filesystem::exists(capDirectory / "1.part") && std::filesystem::exists(capDirectory / "2.part"));
        AKK_CLUSTER_CHECK(capBudget->stats().discardedPartials == 1 && capBudget->stats().retainedPartials == 2);
        std::error_code cleanupError;
        std::filesystem::remove_all(dir / "akkaradb-transfer-resume-v1", cleanupError);
        AKK_CLUSTER_CHECK(!cleanupError);
        message.reset();
        AKK_CLUSTER_CHECK(sender->used(transfer::TransferBudget::Resource::SPOOL) == 0);
        AKK_CLUSTER_CHECK(sender->used(transfer::TransferBudget::Resource::MEMORY) == 0);
        AKK_CLUSTER_CHECK(std::ranges::none_of(std::filesystem::recursive_directory_iterator{dir}, [](const auto& entry) {
            return entry.is_regular_file();
        }));
        auto tinyOptions = options;
        tinyOptions.thresholdBytes = 1;
        auto tinyBudget = std::make_shared<transfer::TransferBudget>(tinyOptions);
        const auto ack = transfer::messageFromWire(encodeAck(ReplAck{.seq = 1, .stage = AckStage::DURABLE}), tinyBudget);
        unsigned controls = 0;
        AKK_CLUSTER_CHECK(transfer::sendMessage(*ack, tinyBudget, [&](auto wire) {
            DecodedFrame frame;
            ++controls;
            return decodeFrame(wire, frame) && frame.type == ReplMsgType::ACK;
        }));
        AKK_CLUSTER_CHECK(controls == 1 && tinyBudget->used(transfer::TransferBudget::Resource::SPOOL) == 0);
        for (const auto resource : {transfer::TransferBudget::Resource::MEMORY, transfer::TransferBudget::Resource::SPOOL}) {
            const auto limit = resource == transfer::TransferBudget::Resource::MEMORY ? options.maxMemoryBytes : options.maxSpoolBytes;
            bool rejected = false;
            try { (void)sender->reserve(resource, limit + 1); }
            catch (const std::runtime_error&) { rejected = true; }
            AKK_CLUSTER_CHECK(rejected && sender->used(resource) == 0);
        }
    }

    void testLargeReplicationTransfer(bool secure) {
        namespace transfer = akkaradb::engine::cluster::detail;
        const auto dir = makeTempDir(secure ? "large-secure-transfer" : "large-plain-transfer");
        ClusterRuntimeOptions options;
        options.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
        options.transfer.thresholdBytes = 4096;
        options.transfer.chunkBytes = 8192;
        options.transfer.spoolDirectory = dir / "spools";
        auto budget = std::make_shared<transfer::TransferBudget>(options.transfer);
        if (secure) {
            for (uint64_t id : {1, 2, 3}) {
                const auto identity = akkaradb::crypto::IdentityStore{dir / std::to_string(id)}.loadOrCreate();
                options.secure.pinnedPeers.push_back({id, identity.publicKey});
            }
            options.secure.identitySeedPath = dir / "1";
        }
        const AckPolicy ack{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::DURABLE};
        const std::string value(1024 * 1024, 'L');
        auto server = ReplicationServer::create(23202, 1, [] { return uint64_t{0}; }, ack,
            ConsistencyOptions{.ackTimeoutAction = AckTimeoutAction::FAIL_WRITE, .ackTimeoutMs = 5000}, 2, {2, 3}, options, {}, {}, budget);
        server->setReadCallback([&](const ReadRequest&) {
            ReadResponse response;
            response.status = ReadStatus::FOUND;
            response.value.assign(value.begin(), value.end());
            return response;
        });
        std::atomic<unsigned> applied{0};
        std::vector<std::unique_ptr<ReplicationClient>> clients;
        for (uint64_t id : {2, 3}) {
            auto clientOptions = options;
            clientOptions.secure.identitySeedPath = dir / std::to_string(id);
            clientOptions.secure.expectedPrimaryNodeId = 1;
            auto client = ReplicationClient::create("127.0.0.1", 23202, id, [] { return uint64_t{0}; }, ack, clientOptions);
            client->setApplyCallback([&](uint64_t seq, ReplOpType op, auto key, auto data, uint8_t, uint64_t) {
                AKK_CLUSTER_CHECK(seq == 1 && op == ReplOpType::PUT && textOf(key) == "large" && textOf(data) == value);
                applied.fetch_add(1);
            });
            client->setForceDurableCallback([] {});
            clients.push_back(std::move(client));
        }
        server->start();
        for (auto& client : clients) { client->start(); }
        AKK_CLUSTER_CHECK(waitUntil([&] { return server->replicaCount() == 2; }));
        AKK_CLUSTER_CHECK(server->stats().connectedReplicas == 2);
        server->shipEntry(1, ReplOpType::PUT, bytesOf("large"), bytesOf(value), 0, 1);
        AKK_CLUSTER_CHECK(applied.load() == 2);
        // Both queues and the retained history share one temporary payload.
        AKK_CLUSTER_CHECK(waitUntil([&] {
            return budget->used(transfer::TransferBudget::Resource::SPOOL) == value.size() + 31;
        }));
        const auto response = clients.front()->readKey(bytesOf("large"), 0, 5000);
        AKK_CLUSTER_CHECK(response.status == ReadStatus::FOUND && textOf(response.value) == value);
        for (auto& client : clients) { client->close(); }
        server->close();
        AKK_CLUSTER_CHECK(budget->used(transfer::TransferBudget::Resource::SPOOL) == 0);
        AKK_CLUSTER_CHECK(std::ranges::none_of(std::filesystem::recursive_directory_iterator{options.transfer.spoolDirectory}, [](const auto& entry) {
            return entry.is_regular_file();
        }));
    }

    void testTimedOutReadKeepsWriteConnection() {
        ClusterRuntimeOptions options;
        options.transportMode = TransportMode::PLAIN;
        const AckPolicy ack{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::DURABLE};
        auto server = ReplicationServer::create(23203, 1, [] { return uint64_t{0}; }, ack,
            ConsistencyOptions{.ackTimeoutAction = AckTimeoutAction::FAIL_WRITE, .ackTimeoutMs = 1000}, 1, {2}, options);
        std::promise<void> release;
        auto released = release.get_future().share();
        std::promise<void> entered;
        auto started = entered.get_future();
        std::atomic<unsigned> calls{0};
        server->setReadCallback([&](const ReadRequest&) {
            if (calls.fetch_add(1) == 0) {
                entered.set_value();
                (void)released.wait_for(std::chrono::seconds{5});
            }
            ReadResponse response;
            response.status = ReadStatus::FOUND;
            return response;
        });
        auto client = ReplicationClient::create("127.0.0.1", 23203, 2, [] { return uint64_t{0}; }, ack, options);
        std::atomic<unsigned> applied{0};
        client->setApplyCallback([&](uint64_t, ReplOpType, auto, auto, uint8_t, uint64_t) { applied.fetch_add(1); });
        client->setForceDurableCallback([] {});
        server->start(); client->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return server->replicaCount() == 1 && client->connected(); }));
        for (unsigned i = 0; i < 4; ++i) {
            bool timedOut = false;
            try { (void)client->readKey(bytesOf("blocked"), 0, 30); }
            catch (const std::runtime_error&) { timedOut = true; }
            AKK_CLUSTER_CHECK(timedOut);
        }
        AKK_CLUSTER_CHECK(started.wait_for(std::chrono::seconds{1}) == std::future_status::ready);
        server->shipEntry(1, ReplOpType::PUT, bytesOf("write"), bytesOf("ok"), 0, 1);
        AKK_CLUSTER_CHECK(applied.load() == 1 && calls.load() == 1);
        release.set_value();
        AKK_CLUSTER_CHECK(client->readKey(bytesOf("next"), 0, 1000).status == ReadStatus::FOUND);
        AKK_CLUSTER_CHECK(calls.load() == 2);
        client->close(); server->close();
    }

    void testClusterRuntimeDoesNotSerializePeerReadWithStats() {
        const auto dir = makeTempDir("runtime-peer-read-lock");
        const ClusterConfig cfg{
            {
                node(1, 23501, 23511),
                node(2, 23502, 23512),
            },
            ReplicationMode::PARTITIONED,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 3000},
        };
        ClusterRuntimeOptions options;
        options.transportMode = TransportMode::PLAIN;
        options.clusterGroupId = 9021;
        options.clusterGroupEpoch = 1;

        auto peer = ReplicationServer::create(23512, 2, [] { return uint64_t{0}; }, {},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 3000}, 1, {1}, options);
        std::promise<void> entered;
        auto enteredFuture = entered.get_future();
        std::promise<void> release;
        auto released = release.get_future().share();
        peer->setReadCallback([&](const ReadRequest&) {
            entered.set_value();
            (void)released.wait_for(std::chrono::seconds{5});
            ReadResponse response;
            response.status = ReadStatus::FOUND;
            response.value = {'o', 'k'};
            return response;
        });
        peer->start();

        auto local = makeRuntime(dir / "n1", cfg, 1, options);
        local->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return peer->replicaCount() == 1; }));
        auto read = std::async(std::launch::async, [&] {
            return local->runtime->readKeyFromNode(2, bytesOf("blocked"), 0);
        });
        AKK_CLUSTER_CHECK(enteredFuture.wait_for(std::chrono::seconds{1}) == std::future_status::ready);
        auto stats = std::async(std::launch::async, [&] { return local->runtime->raftStats(); });
        const auto statsStatus = stats.wait_for(std::chrono::milliseconds{250});
        release.set_value();
        const auto response = read.get();
        const auto observed = stats.get();
        AKK_CLUSTER_CHECK(statsStatus == std::future_status::ready);
        AKK_CLUSTER_CHECK(response.status == ReadStatus::FOUND && textOf(response.value) == "ok");
        AKK_CLUSTER_CHECK(observed.peers.size() == 1);
        AKK_CLUSTER_CHECK(observed.peers.front().nodeId == 2);
        AKK_CLUSTER_CHECK(observed.peers.front().lastSuccessfulContactAtUs != 0);
        const auto completed = local->runtime->raftStats();
        AKK_CLUSTER_CHECK(completed.sampledAtUs >= completed.runtimeStartedAtUs && completed.runtimeStartedAtUs != 0);
        AKK_CLUSTER_CHECK(completed.peers.size() == 1);
        AKK_CLUSTER_CHECK(completed.peers.front().roundTripsSucceeded == 1);
        AKK_CLUSTER_CHECK(completed.peers.front().roundTripsFailed == 0);
        AKK_CLUSTER_CHECK(completed.peers.front().lastRoundTripAtUs != 0);
        AKK_CLUSTER_CHECK(completed.peers.front().roundTripLatencyUs.sampleCount == 1);
        AKK_CLUSTER_CHECK(completed.peers.front().roundTripLatencyUs.bucketCounts.back() == 1);
        local->runtime->close();
        peer->close();
    }

    void testClusterRuntimeStartCanRetryAfterFailure() {
        const auto dir = makeTempDir("runtime-start-retry");
        const ClusterConfig cfg{
            {
                node(1, 23521, 23531),
                node(2, 23522, 23532),
            },
            ReplicationMode::PARTITIONED,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 500},
        };
        ClusterRuntimeOptions options;
        options.transportMode = TransportMode::PLAIN;
        options.clusterGroupId = 9022;
        options.clusterGroupEpoch = 1;
        options.clusterMembershipPath = dir / "blocked-membership-state";

        std::filesystem::create_directory(options.clusterMembershipPath);
        auto runtime = makeRuntime(dir / "n1", cfg, 1, options);
        bool rejected = false;
        try { runtime->runtime->start(); }
        catch (const std::runtime_error&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected);
        AKK_CLUSTER_CHECK(std::filesystem::remove(options.clusterMembershipPath));

        runtime->runtime->start();
        AKK_CLUSTER_CHECK(runtime->runtime->role() == NodeRole::PRIMARY);
        runtime->runtime->close();
    }

    void testClusterRuntimeReportsEndpointStartFailure() {
        const auto dir = makeTempDir("runtime-endpoint-start-stats");
        const ClusterConfig cfg{
            {node(1, 23621, 23631), node(2, 23622, 23632)},
            ReplicationMode::PARTITIONED,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 100},
        };
        ClusterRuntimeOptions options;
        options.transportMode = TransportMode::PLAIN;
        options.clusterGroupId = 9023;
        options.clusterGroupEpoch = 1;
        options.replBindHost = "not a valid bind host";
        auto runtime = makeRuntime(dir / "n1", cfg, 1, options);
        bool rejected = false;
        try { runtime->runtime->start(); }
        catch (const std::runtime_error&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected);
        const auto stats = runtime->runtime->raftStats();
        AKK_CLUSTER_CHECK(stats.health == akkaradb::engine::ClusterHealthState::FAILED);
        AKK_CLUSTER_CHECK(stats.lastFailure == akkaradb::engine::ClusterFailureCode::ENDPOINT_START);
        AKK_CLUSTER_CHECK(stats.lastFailureAtUs != 0 && stats.endpointStartFailures == 1);
        runtime->runtime->close();
    }

    void testClusterRuntimeReportsPeerReadTimeout() {
        const auto dir = makeTempDir("runtime-peer-read-timeout-stats");
        const ClusterConfig cfg{
            {node(1, 23641, 23651), node(2, 23642, 23652)},
            ReplicationMode::PARTITIONED,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 100},
        };
        ClusterRuntimeOptions options;
        options.transportMode = TransportMode::PLAIN;
        options.clusterGroupId = 9024;
        options.clusterGroupEpoch = 1;
        auto runtime = makeRuntime(dir / "n1", cfg, 1, options);
        runtime->runtime->start();
        const auto response = runtime->runtime->readKeyFromNode(2, bytesOf("unreachable"), 0);
        AKK_CLUSTER_CHECK(response.status == ReadStatus::ERROR_STATUS);
        const auto stats = runtime->runtime->raftStats();
        AKK_CLUSTER_CHECK(stats.health == akkaradb::engine::ClusterHealthState::DEGRADED);
        AKK_CLUSTER_CHECK(stats.lastFailure == akkaradb::engine::ClusterFailureCode::PEER_READ_TIMEOUT);
        AKK_CLUSTER_CHECK(stats.lastFailureAtUs != 0 && stats.peerReadTimeouts == 1);
        AKK_CLUSTER_CHECK(stats.peers.size() == 1 && !stats.peers.front().connected);
        AKK_CLUSTER_CHECK(stats.peers.front().roundTripsSucceeded == 0);
        AKK_CLUSTER_CHECK(stats.peers.front().roundTripsFailed == 1);
        AKK_CLUSTER_CHECK(stats.peers.front().consecutiveRoundTripFailures == 1);
        AKK_CLUSTER_CHECK(stats.peers.front().lastRoundTripAtUs != 0);
        AKK_CLUSTER_CHECK(stats.peers.front().lastRoundTripFailureAtUs != 0);
        AKK_CLUSTER_CHECK(stats.peers.front().roundTripLatencyUs.sampleCount == 0);
        runtime->runtime->close();
    }

    void testRetrySafeEngineWrites() {
        const auto dir = makeTempDir("retry-safe-engine");
        akkaradb::engine::AkkEngineOptions options;
        options.paths.dataDir = dir;
        options.components.clusterEnabled = true;
        options.components.blobEnabled = true;
        options.blob.thresholdBytes = 128;
        options.cluster.config = ClusterConfig{{node(1, 23101, 23201)}, ReplicationMode::MIRROR, {},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 2000}};
        options.cluster.runtime.requests.enabled = true;
        options.cluster.runtime.requests.maxTrackedRequests = 2;
        options.cluster.runtime.requests.maxRetentionMs = 60'000;
        options.cluster.runtime.raftBlobPolicy = RaftBlobPolicy::RAFT_LOG;
        options.cluster.runtime.raftSnapshot.minLogEntries = 1;
        options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        writeNodeIdFile(dir / "node.id", 1);
        auto engine = akkaradb::engine::AkkEngine::open(options);
        AKK_CLUSTER_CHECK(waitUntil([&] { return engine->stats().cluster.role == static_cast<uint32_t>(NodeRole::PRIMARY); }));
        const auto id = engine->newRequestId();
        AKK_CLUSTER_CHECK(engine->queryRequest(id).status == ClusterRequestStatus::NOT_FOUND);
        const std::string value(4096, 'v');
        std::vector<std::future<ClusterRequestResult>> retries;
        for (unsigned i = 0; i < 8; ++i) {
            retries.push_back(std::async(std::launch::async, [&] { return engine->putWithRequest(id, bytesOf("once"), bytesOf(value)); }));
        }
        const auto first = retries.front().get();
        AKK_CLUSTER_CHECK(first.status == ClusterRequestStatus::APPLIED);
        for (size_t i = 1; i < retries.size(); ++i) {
            const auto result = retries[i].get();
            AKK_CLUSTER_CHECK(result.sequence == first.sequence && result.logIndex == first.logIndex);
        }
        auto observed = engine->stats();
        AKK_CLUSTER_CHECK(observed.putsTotal == 1);
        AKK_CLUSTER_CHECK(observed.cluster.health == akkaradb::engine::ClusterHealthState::HEALTHY);
        AKK_CLUSTER_CHECK(observed.cluster.lastFailure == akkaradb::engine::ClusterFailureCode::NONE);
        AKK_CLUSTER_CHECK(observed.cluster.lastFailureAtUs == 0);
        AKK_CLUSTER_CHECK(observed.cluster.sampledAtUs >= observed.cluster.runtimeStartedAtUs);
        AKK_CLUSTER_CHECK(observed.cluster.runtimeStartedAtUs != 0);
        AKK_CLUSTER_CHECK(observed.cluster.clusterId == options.cluster.config->clusterId());
        AKK_CLUSTER_CHECK(observed.cluster.replicationMode == static_cast<uint32_t>(ReplicationMode::MIRROR));
        AKK_CLUSTER_CHECK(observed.cluster.consistencyMode == static_cast<uint32_t>(ConsistencyMode::RAFT_QUORUM));
        AKK_CLUSTER_CHECK(observed.cluster.transportMode == static_cast<uint32_t>(options.cluster.runtime.transportMode));
        AKK_CLUSTER_CHECK(observed.cluster.configuredNodes.size() == 1);
        AKK_CLUSTER_CHECK(observed.cluster.configuredNodes.front().nodeId == 1);
        AKK_CLUSTER_CHECK(observed.cluster.configuredNodes.front().replPort == 23201);
        AKK_CLUSTER_CHECK(observed.cluster.raftEnabled && observed.cluster.raftTerm != 0 && observed.cluster.leaderNodeId == 1);
        AKK_CLUSTER_CHECK(observed.cluster.commitIndex >= first.logIndex && observed.cluster.appliedIndex >= first.logIndex);
        AKK_CLUSTER_CHECK(observed.cluster.retainedRequestResults == 1 && observed.cluster.requestCapacity == 2);
        AKK_CLUSTER_CHECK(observed.cluster.requestJournalRecords >= 1 && observed.cluster.requestJournalBytes > 0);
        AKK_CLUSTER_CHECK(observed.cluster.requestJournalBytesWrittenTotal == observed.cluster.requestJournalBytes);
        bool conflict = false;
        try { (void)engine->putWithRequest(id, bytesOf("once"), bytesOf("different")); }
        catch (const std::invalid_argument&) { conflict = true; }
        AKK_CLUSTER_CHECK(conflict);
        const auto expired = engine->newRequestId(1);
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
        AKK_CLUSTER_CHECK(engine->putWithRequest(expired, bytesOf("never"), bytesOf("value")).status == ClusterRequestStatus::EXPIRED);
        AKK_CLUSTER_CHECK(!engineGet(*engine, "never"));
        engine->put(bytesOf("once"), bytesOf("newer"));
        AKK_CLUSTER_CHECK(waitUntil([&] { return readRaftLogLastIncludedIndex(dir / "cluster-raft.log") > first.logIndex; }));
        engine->close();
        engine = akkaradb::engine::AkkEngine::open(options);
        AKK_CLUSTER_CHECK(waitUntil([&] { return engine->stats().cluster.role == static_cast<uint32_t>(NodeRole::PRIMARY); }));
        const auto result = engine->putWithRequest(id, bytesOf("once"), bytesOf(value));
        AKK_CLUSTER_CHECK(result.sequence == first.sequence && result.logIndex == first.logIndex);
        AKK_CLUSTER_CHECK(textOf(*engineGet(*engine, "once")) == "newer");
        AKK_CLUSTER_CHECK(engine->stats().putsTotal == 0);
        AKK_CLUSTER_CHECK(engine->queryRequest(id).status == ClusterRequestStatus::APPLIED);
        const auto removeId = engine->newRequestId();
        AKK_CLUSTER_CHECK(engine->removeWithRequest(removeId, bytesOf("once")).status == ClusterRequestStatus::APPLIED);
        AKK_CLUSTER_CHECK(engine->removeWithRequest(removeId, bytesOf("once")).status == ClusterRequestStatus::APPLIED);
        AKK_CLUSTER_CHECK(engine->stats().removesTotal == 1);
        observed = engine->stats();
        AKK_CLUSTER_CHECK(observed.cluster.retainedRequestResults == 2);
        AKK_CLUSTER_CHECK(observed.cluster.requestJournalRecords >= 2);
        AKK_CLUSTER_CHECK(observed.cluster.requestJournalBytesWrittenTotal < observed.cluster.requestJournalBytes * 2);
        bool full = false;
        try { (void)engine->putWithRequest(engine->newRequestId(), bytesOf("overflow"), bytesOf("no")); }
        catch (const std::runtime_error&) { full = true; }
        AKK_CLUSTER_CHECK(full && !engineGet(*engine, "overflow"));
        engine->close();
        std::filesystem::path requestJournal;
        for (const auto& file : std::filesystem::directory_iterator(dir)) {
            if (file.is_regular_file() && file.path().filename().string().starts_with("cluster-raft.requests.")) {
                requestJournal = file.path();
                break;
            }
        }
        AKK_CLUSTER_CHECK(!requestJournal.empty());
        corruptFileByte(requestJournal, 10);
        bool corruptRejected = false;
        try { (void)akkaradb::engine::AkkEngine::open(options); }
        catch (const std::runtime_error& error) {
            corruptRejected = std::string_view{error.what()}.find("request journal") != std::string_view::npos;
        }
        AKK_CLUSTER_CHECK(corruptRejected);
    }

    void testRaftRejectsRequestPolicyMismatchAndReportsStats() {
        const auto root = makeTempDir("raft-request-policy-mismatch");
        const ClusterConfig cfg{
            {node(1, 23301, 23401), node(2, 23302, 23402), node(3, 23303, 23403)},
            ReplicationMode::MIRROR,
            {},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 2000},
        };
        ClusterRuntimeOptions matching;
        matching.requests.enabled = true;
        matching.requests.maxRetentionMs = 60'000;
        matching.requests.maxTrackedRequests = 32;
        auto mismatched = matching;
        mismatched.requests.maxTrackedRequests = 33;
        auto n1 = makeRuntime(root / "n1", cfg, 1, matching);
        auto n2 = makeRuntime(root / "n2", cfg, 2, matching);
        auto n3 = makeRuntime(root / "n3", cfg, 3, mismatched);
        n1->runtime->start(); n2->runtime->start(); n3->runtime->start();
        RuntimeHarness* compatible[] = {n1.get(), n2.get()};
        AKK_CLUSTER_CHECK(waitUntil([&] { return findLeader(compatible) != nullptr; }, std::chrono::milliseconds{15000}));
        auto* leader = findLeader(compatible);
        AKK_CLUSTER_CHECK(leader != nullptr && n3->runtime->role() != NodeRole::PRIMARY);
        leader->runtime->shipEntry(1, ReplOpType::PUT, bytesOf("policy"), bytesOf("matched"), 0, leader->nodeId);
        AKK_CLUSTER_CHECK(waitUntil([&] { return n1->hasState(1, "policy", "matched") && n2->hasState(1, "policy", "matched"); }));
        AKK_CLUSTER_CHECK(!n3->hasState(1, "policy", "matched"));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            return n1->runtime->raftStats().peerPolicyMismatchRejects + n2->runtime->raftStats().peerPolicyMismatchRejects +
                   n3->runtime->raftStats().peerPolicyMismatchRejects > 0;
        }));
        bool policyFailureObserved = false;
        for (const auto* node : compatible) {
            const auto observed = node->runtime->raftStats();
            if (observed.peerPolicyMismatchRejects == 0) { continue; }
            policyFailureObserved = true;
            AKK_CLUSTER_CHECK(observed.health == akkaradb::engine::ClusterHealthState::DEGRADED);
            AKK_CLUSTER_CHECK(observed.lastFailure == akkaradb::engine::ClusterFailureCode::POLICY_MISMATCH);
            AKK_CLUSTER_CHECK(observed.lastFailureAtUs != 0);
        }
        AKK_CLUSTER_CHECK(policyFailureObserved);
        const auto stats = leader->runtime->raftStats();
        AKK_CLUSTER_CHECK(stats.enabled && stats.currentTerm != 0 && stats.leaderNodeId == leader->nodeId);
        AKK_CLUSTER_CHECK(stats.commitIndex >= 1 && stats.appliedIndex >= 1 && stats.lastLogIndex >= stats.commitIndex);
        AKK_CLUSTER_CHECK(stats.peers.size() == 2 && stats.requestCapacity == 32);
        n3->runtime->close(); n2->runtime->close(); n1->runtime->close();
    }

    void testRaftRejectsForeignClusterIdentity() {
        const auto root = makeTempDir("raft-cluster-identity-mismatch");
        const std::vector<NodeInfo> nodes{
            node(1, 23541, 23551), node(2, 23542, 23552), node(3, 23543, 23553),
        };
        const ClusterConfig localCfg{
            nodes, ReplicationMode::MIRROR, {},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 2000},
        };
        const ClusterConfig foreignCfg{
            nodes, ReplicationMode::MIRROR, {},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 2000},
        };
        AKK_CLUSTER_CHECK(localCfg.clusterId() != foreignCfg.clusterId());
        auto n1 = makeRuntime(root / "n1", localCfg, 1);
        auto n2 = makeRuntime(root / "n2", localCfg, 2);
        auto n3 = makeRuntime(root / "n3", foreignCfg, 3);
        n1->runtime->start(); n2->runtime->start(); n3->runtime->start();
        RuntimeHarness* compatible[] = {n1.get(), n2.get()};
        AKK_CLUSTER_CHECK(waitUntil([&] { return findLeader(compatible) != nullptr; }, std::chrono::milliseconds{15000}));
        auto* leader = findLeader(compatible);
        AKK_CLUSTER_CHECK(leader != nullptr && n3->runtime->role() != NodeRole::PRIMARY);
        leader->runtime->shipEntry(1, ReplOpType::PUT, bytesOf("identity"), bytesOf("local"), 0, leader->nodeId);
        AKK_CLUSTER_CHECK(waitUntil([&] {
            return n1->hasState(1, "identity", "local") && n2->hasState(1, "identity", "local");
        }));
        AKK_CLUSTER_CHECK(!n3->hasState(1, "identity", "local"));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            return n1->runtime->raftStats().foreignClusterRejects + n2->runtime->raftStats().foreignClusterRejects +
                   n3->runtime->raftStats().foreignClusterRejects > 0;
        }));
        bool foreignFailureObserved = false;
        for (const auto* node : compatible) {
            const auto observed = node->runtime->raftStats();
            if (observed.foreignClusterRejects == 0) { continue; }
            foreignFailureObserved = true;
            AKK_CLUSTER_CHECK(observed.health == akkaradb::engine::ClusterHealthState::DEGRADED);
            AKK_CLUSTER_CHECK(observed.lastFailure == akkaradb::engine::ClusterFailureCode::FOREIGN_CLUSTER);
            AKK_CLUSTER_CHECK(observed.lastFailureAtUs != 0);
        }
        AKK_CLUSTER_CHECK(foreignFailureObserved);
        n3->runtime->close(); n2->runtime->close(); n1->runtime->close();
    }

    void testRaftRejectsBlobPolicyMismatch() {
        const auto root = makeTempDir("raft-blob-policy-mismatch");
        const ClusterConfig cfg{
            {node(1, 23561, 23571), node(2, 23562, 23572), node(3, 23563, 23573)},
            ReplicationMode::MIRROR,
            {},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 2000},
        };
        ClusterRuntimeOptions matching;
        auto mismatched = matching;
        mismatched.raftBlobPolicy = RaftBlobPolicy::PRIMARY_SIDE_ONLY;
        auto n1 = makeRuntime(root / "n1", cfg, 1, matching);
        auto n2 = makeRuntime(root / "n2", cfg, 2, matching);
        auto n3 = makeRuntime(root / "n3", cfg, 3, mismatched);
        n1->runtime->start(); n2->runtime->start(); n3->runtime->start();
        RuntimeHarness* compatible[] = {n1.get(), n2.get()};
        AKK_CLUSTER_CHECK(waitUntil([&] { return findLeader(compatible) != nullptr; }, std::chrono::milliseconds{15000}));
        auto* leader = findLeader(compatible);
        AKK_CLUSTER_CHECK(leader != nullptr && n3->runtime->role() != NodeRole::PRIMARY);
        leader->runtime->shipEntry(1, ReplOpType::PUT, bytesOf("blob-policy"), bytesOf("matched"), 0, leader->nodeId);
        AKK_CLUSTER_CHECK(waitUntil([&] {
            return n1->hasState(1, "blob-policy", "matched") && n2->hasState(1, "blob-policy", "matched");
        }));
        AKK_CLUSTER_CHECK(!n3->hasState(1, "blob-policy", "matched"));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            return n1->runtime->raftStats().peerPolicyMismatchRejects +
                   n2->runtime->raftStats().peerPolicyMismatchRejects +
                   n3->runtime->raftStats().peerPolicyMismatchRejects > 0;
        }));
        const auto observed = leader->runtime->raftStats();
        AKK_CLUSTER_CHECK(observed.health == akkaradb::engine::ClusterHealthState::DEGRADED);
        AKK_CLUSTER_CHECK(observed.lastFailure == akkaradb::engine::ClusterFailureCode::POLICY_MISMATCH);
        AKK_CLUSTER_CHECK(observed.lastFailureAtUs != 0);
        n3->runtime->close(); n2->runtime->close(); n1->runtime->close();
    }

    void testRequestJournalDeltaCompaction() {
        ScopedRequestJournalCompactionThreshold compactEvery{"3"};
        const auto dir = makeTempDir("request-journal-compaction");
        akkaradb::engine::AkkEngineOptions options;
        options.paths.dataDir = dir;
        options.components.clusterEnabled = true;
        options.components.blobEnabled = false;
        options.cluster.config = ClusterConfig{{node(1, 23311, 23411)}, ReplicationMode::MIRROR, {},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 2000}};
        options.cluster.runtime.requests.enabled = true;
        options.cluster.runtime.requests.maxTrackedRequests = 16;
        options.cluster.runtime.requests.maxRetentionMs = 60'000;
        writeNodeIdFile(dir / "node.id", 1);
        auto engine = akkaradb::engine::AkkEngine::open(options);
        AKK_CLUSTER_CHECK(waitUntil([&] { return engine->stats().cluster.role == static_cast<uint32_t>(NodeRole::PRIMARY); }));
        std::vector<ClusterRequestId> ids;
        std::vector<ClusterRequestResult> receipts;
        for (unsigned i = 0; i < 6; ++i) {
            ids.push_back(engine->newRequestId());
            receipts.push_back(engine->putWithRequest(ids.back(), bytesOf("journal-" + std::to_string(i)), bytesOf("value")));
        }
        const auto stats = engine->stats().cluster;
        AKK_CLUSTER_CHECK(stats.retainedRequestResults == 6 && stats.requestJournalCompactionsTotal >= 2);
        AKK_CLUSTER_CHECK(stats.requestJournalRecords <= 3 && stats.requestJournalBytesWrittenTotal > stats.requestJournalBytes);
        engine->close();
        engine = akkaradb::engine::AkkEngine::open(options);
        AKK_CLUSTER_CHECK(waitUntil([&] { return engine->stats().cluster.role == static_cast<uint32_t>(NodeRole::PRIMARY); }));
        const auto retry = engine->putWithRequest(ids.front(), bytesOf("journal-0"), bytesOf("value"));
        AKK_CLUSTER_CHECK(retry.sequence == receipts.front().sequence && retry.logIndex == receipts.front().logIndex);
        engine->close();
    }

    akkaradb::engine::AkkEngineOptions retryCrashOptions(const std::filesystem::path& dir) {
        akkaradb::engine::AkkEngineOptions options;
        options.paths.dataDir = dir;
        options.components.clusterEnabled = true;
        options.components.blobEnabled = false;
        const auto configPath = dir / "cluster.cfg";
        options.cluster.config = std::filesystem::exists(configPath)
                                     ? ClusterConfig::load(configPath)
                                     : ClusterConfig{{node(1, 23111, 23211)}, ReplicationMode::MIRROR, {},
                                           ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 2000}};
        if (!std::filesystem::exists(configPath)) { ClusterConfig::save(configPath, *options.cluster.config); }
        options.cluster.runtime.requests.enabled = true;
        writeNodeIdFile(dir / "node.id", 1);
        return options;
    }

    int runRetryCrashWriter(const char* point, const std::filesystem::path& dir) {
        auto engine = akkaradb::engine::AkkEngine::open(retryCrashOptions(dir));
        AKK_CLUSTER_CHECK(waitUntil([&] { return engine->stats().cluster.role == static_cast<uint32_t>(NodeRole::PRIMARY); }));
        const auto id = engine->newRequestId();
        // Complete a read round before enabling the fault so the election NOOP is applied.
        AKK_CLUSTER_CHECK(engine->queryRequest(id).status == ClusterRequestStatus::NOT_FOUND);
        {
            std::ofstream token{dir / "request-token", std::ios::binary};
            token.write(reinterpret_cast<const char*>(id.nonce.data()), id.nonce.size());
            token.write(reinterpret_cast<const char*>(&id.expiresAtUnixMs), sizeof(id.expiresAtUnixMs));
        }
        setStorageCrashPoint(point);
        (void)engine->putWithRequest(id, bytesOf("crash-once"), bytesOf("first"));
        return 1;
    }

    void testRetryAfterApplyCrash(const std::filesystem::path& executable) {
        for (const char* point : {"raft.request.after_apply", "raft.request.after_journal_sync"}) {
            const auto dir = makeTempDir(std::string{"retry-crash-"} + (std::string_view{point}.ends_with("sync") ? "journal" : "apply"));
            runStorageCrashChild(executable, "--retry-crash-writer", point, dir);
            ClusterRequestId id;
            std::ifstream token{dir / "request-token", std::ios::binary};
            token.read(reinterpret_cast<char*>(id.nonce.data()), id.nonce.size());
            token.read(reinterpret_cast<char*>(&id.expiresAtUnixMs), sizeof(id.expiresAtUnixMs));
            AKK_CLUSTER_CHECK(token.good());
            auto engine = akkaradb::engine::AkkEngine::open(retryCrashOptions(dir));
            AKK_CLUSTER_CHECK(waitUntil([&] { return engine->stats().cluster.role == static_cast<uint32_t>(NodeRole::PRIMARY); }));
            const auto receipt = engine->queryRequest(id);
            AKK_CLUSTER_CHECK(receipt.status == ClusterRequestStatus::APPLIED);
            engine->put(bytesOf("crash-once"), bytesOf("later"));
            const auto retry = engine->putWithRequest(id, bytesOf("crash-once"), bytesOf("first"));
            AKK_CLUSTER_CHECK(retry.sequence == receipt.sequence && retry.logIndex == receipt.logIndex);
            AKK_CLUSTER_CHECK(textOf(*engineGet(*engine, "crash-once")) == "later");
            engine->close();
        }
    }

    void testRetryAcrossSnapshotAndLeaderChange() {
        const auto dir = makeTempDir("retry-snapshot-leader");
        const ClusterConfig cfg{{node(1, 23121, 23221), node(2, 23122, 23222), node(3, 23123, 23223)},
            ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 2000}};
        auto optionsFor = [&](uint64_t id) {
            akkaradb::engine::AkkEngineOptions options;
            options.paths.dataDir = dir / ("n" + std::to_string(id));
            options.components.clusterEnabled = true;
            options.components.blobEnabled = false;
            options.cluster.config = cfg;
            options.cluster.runtime.transportMode = TransportMode::PLAIN;
            options.cluster.runtime.requests.enabled = true;
            options.cluster.runtime.raftSnapshot.minLogEntries = 1;
            writeNodeIdFile(options.paths.dataDir / "node.id", id);
            return options;
        };
        auto n1 = akkaradb::engine::AkkEngine::open(optionsFor(1));
        auto n2 = akkaradb::engine::AkkEngine::open(optionsFor(2));
        akkaradb::engine::AkkEngine* initial[] = {n1.get(), n2.get()};
        ClusterRequestId id;
        ClusterRequestResult first;
        runOnEngineLeader(initial, [&](auto& leader) {
            if (id.expiresAtUnixMs == 0) { id = leader.newRequestId(); }
            first = leader.putWithRequest(id, bytesOf("snapshot-once"), bytesOf("first"));
        });
        runOnEngineLeader(initial, [&](auto& leader) { leader.put(bytesOf("snapshot-once"), bytesOf("later")); });
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto* leader = findEngineLeader(initial);
            if (!leader) { return false; }
            return readRaftLogLastIncludedIndex(dir / (leader == n1.get() ? "n1" : "n2") / "cluster-raft.log") > first.logIndex;
        }));
        auto n3 = akkaradb::engine::AkkEngine::open(optionsFor(3));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            auto value = engineGet(*n3, "snapshot-once");
            return value && textOf(*value) == "later";
        }, std::chrono::milliseconds{20000}));
        akkaradb::engine::AkkEngine* all[] = {n1.get(), n2.get(), n3.get()};
        runOnEngineLeader(all, [&](auto& leader) { leader.transferClusterLeadership(3); });
        AKK_CLUSTER_CHECK(waitUntil([&] { return findEngineLeader(all) == n3.get(); }, std::chrono::milliseconds{20000}));
        const auto retry = n3->putWithRequest(id, bytesOf("snapshot-once"), bytesOf("first"));
        AKK_CLUSTER_CHECK(retry.sequence == first.sequence && retry.logIndex == first.logIndex);
        AKK_CLUSTER_CHECK(textOf(*engineGet(*n3, "snapshot-once")) == "later");
        AKK_CLUSTER_CHECK(n3->queryRequest(id).status == ClusterRequestStatus::APPLIED);
        n1->close();
        n2->close();
        const auto timedId = n3->newRequestId();
        bool failed = false;
        try { (void)n3->putWithRequest(timedId, bytesOf("timeout-once"), bytesOf("first")); }
        catch (const std::runtime_error&) { failed = true; }
        AKK_CLUSTER_CHECK(failed);
        bool queryFailed = false;
        try { (void)n3->queryRequest(timedId); }
        catch (const std::runtime_error&) { queryFailed = true; }
        AKK_CLUSTER_CHECK(queryFailed);
        n1 = akkaradb::engine::AkkEngine::open(optionsFor(1));
        n2 = akkaradb::engine::AkkEngine::open(optionsFor(2));
        akkaradb::engine::AkkEngine* recovered[] = {n1.get(), n2.get(), n3.get()};
        ClusterRequestResult timedReceipt;
        runOnEngineLeader(recovered, [&](auto& leader) {
            timedReceipt = leader.putWithRequest(timedId, bytesOf("timeout-once"), bytesOf("first"));
        });
        runOnEngineLeader(recovered, [&](auto& leader) {
            leader.put(bytesOf("timeout-once"), bytesOf("later"));
            const auto result = leader.putWithRequest(timedId, bytesOf("timeout-once"), bytesOf("first"));
            AKK_CLUSTER_CHECK(result.sequence == timedReceipt.sequence && result.logIndex == timedReceipt.logIndex);
            AKK_CLUSTER_CHECK(textOf(*engineGet(leader, "timeout-once")) == "later");
        });
        n3->close(); n2->close(); n1->close();
    }

    void testRaftConcurrentEngineWrites() {
        const auto dir = makeTempDir("raft-concurrent-engine");
        akkaradb::engine::AkkEngineOptions options;
        options.paths.dataDir = dir;
        options.components.clusterEnabled = true;
        options.components.blobEnabled = true;
        options.blob.thresholdBytes = 128;
        options.cluster.config = ClusterConfig{
            {node(1, 21831, 21931)}, ReplicationMode::MIRROR, AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 2000},
        };
        options.cluster.runtime.raftBlobPolicy = RaftBlobPolicy::RAFT_LOG;
        options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        writeNodeIdFile(dir / "node.id", 1);
        auto engine = akkaradb::engine::AkkEngine::open(options);
        std::vector<std::future<void>> writers;
        for (unsigned w = 0; w < 8; ++w) {
            writers.push_back(std::async(std::launch::async, [&, w] {
                for (unsigned i = 0; i < 16; ++i) {
                    const auto key = std::to_string(w) + ":" + std::to_string(i);
                    engine->put(bytesOf(key), bytesOf(i % 4 == 0 ? std::string(256, 'b') : "inline"));
                }
            }));
        }
        for (auto& writer : writers) { writer.get(); }
        std::vector<std::string> keys;
        for (unsigned i = 0; i < 64; ++i) { keys.push_back("batch-" + std::to_string(i)); }
        std::vector<akkaradb::engine::AkkEngine::BatchPutEntry> batch;
        const std::string batchValue = "batch-value";
        for (const auto& key : keys) { batch.push_back({bytesOf(key), bytesOf(batchValue)}); }
        engine->putBatch(batch);
        for (unsigned w = 0; w < 8; ++w) {
            for (unsigned i = 0; i < 16; ++i) {
                auto value = engine->get(bytesOf(std::to_string(w) + ":" + std::to_string(i)));
                AKK_CLUSTER_CHECK(value && textOf(*value) == (i % 4 == 0 ? std::string(256, 'b') : "inline"));
            }
        }
        for (const auto& key : keys) { AKK_CLUSTER_CHECK(textOf(*engine->get(bytesOf(key))) == "batch-value"); }
        engine->close();
        engine = akkaradb::engine::AkkEngine::open(options);
        AKK_CLUSTER_CHECK(textOf(*engine->get(bytesOf("batch-63"))) == "batch-value");
        engine->close();
    }

    void testRaftHardStateCorruptionFailsStartup() {
        const auto dir = makeTempDir("raft-hard-state-corrupt");
        const ClusterConfig cfg{
            {
                node(1, 20121, 20221),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE},
        };
        ClusterRuntimeOptions options;
        options.transportMode = TransportMode::PLAIN;
        options.corruptStateAction = CorruptClusterStateAction::DELETE_AND_RECREATE;

        {
            auto first = makeRuntime(dir / "n1", cfg, 1, options);
            first->runtime->start();
            AKK_CLUSTER_CHECK(waitUntil([&] { return first->runtime->role() == NodeRole::PRIMARY; }, std::chrono::milliseconds{8000}));
            first->runtime->close();
        }

        const auto statePath = dir / "n1" / "cluster-raft.state";
        AKK_CLUSTER_CHECK(std::filesystem::exists(statePath));
        corruptFileByte(statePath);

        bool rejected = false;
        try {
            auto recovered = makeRuntime(dir / "n1", cfg, 1, options);
            (void)recovered;
        }
        catch (const std::runtime_error& error) {
            rejected = std::string_view{error.what()}.find("corrupt hard state") != std::string_view::npos;
        }
        AKK_CLUSTER_CHECK(rejected);
        AKK_CLUSTER_CHECK(std::filesystem::exists(statePath));
    }

    void testRaftRejectsForeignHardState() {
        const auto dir = makeTempDir("raft-foreign-hard-state");
        const std::vector<NodeInfo> nodes{node(1, 23581, 23591)};
        const ClusterConfig originalCfg{
            nodes,
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE},
        };
        {
            auto original = makeRuntime(dir / "n1", originalCfg, 1);
            original->runtime->start();
            AKK_CLUSTER_CHECK(waitUntil(
                [&] { return original->runtime->role() == NodeRole::PRIMARY; },
                std::chrono::milliseconds{8000}
            ));
            original->runtime->close();
        }

        const ClusterConfig foreignCfg{
            nodes,
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE},
        };
        AKK_CLUSTER_CHECK(originalCfg.clusterId() != foreignCfg.clusterId());
        bool rejected = false;
        try { (void)makeRuntime(dir / "n1", foreignCfg, 1); }
        catch (const std::runtime_error& error) {
            rejected = std::string_view{error.what()}.find("different cluster") != std::string_view::npos;
        }
        AKK_CLUSTER_CHECK(rejected);
    }

    void testRaftDoesNotReplayAppliedEntryAfterRestart() {
        const auto dir = makeTempDir("raft-applied-restart");
        const ClusterConfig cfg{
            {
                node(1, 20141, 20241),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE},
        };

        auto first = makeRuntime(dir / "n1", cfg, 1);
        first->runtime->start();
        RuntimeHarness* nodes[] = {first.get()};
        runOnLeader(
            nodes,
            [](RuntimeHarness& leader) {
                leader.runtime->shipEntry(
                    1,
                    ReplOpType::PUT,
                    bytesOf("applied-restart-key"),
                    bytesOf("applied-restart-value"),
                    0,
                    leader.nodeId
                );
            },
            std::chrono::milliseconds{8000}
        );
        AKK_CLUSTER_CHECK(first->hasState(1, "applied-restart-key", "applied-restart-value"));
        first->runtime->close();
        first.reset();

        auto recovered = makeRuntime(dir / "n1", cfg, 1);
        AKK_CLUSTER_CHECK(recovered->hasState(1, "applied-restart-key", "applied-restart-value"));
        recovered->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return recovered->runtime->role() == NodeRole::PRIMARY; }));
        AKK_CLUSTER_CHECK(recovered->appliedCount.load() == 0);
        recovered->runtime->close();
    }

    void testRaftSnapshotCompactionThresholds() {
        const auto dir = makeTempDir("raft-snapshot-thresholds");
        const ClusterConfig cfg{
            {node(1, 21891, 21991)},
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 500},
        };

        ClusterRuntimeOptions options;
        // A single-node leader appends one NOOP, so three mutations reach four committed entries.
        options.raftSnapshot.minLogEntries = 4;
        options.raftSnapshot.minLogBytes = 1024ull * 1024 * 1024;
        options.raftSnapshot.maxIntervalMs = 60'000;
        auto runtime = makeRuntime(dir / "n1", cfg, 1, options);
        runtime->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return runtime->runtime->role() == NodeRole::PRIMARY; }));

        runtime->runtime->shipEntry(1, ReplOpType::PUT, bytesOf("threshold-1"), bytesOf("value-1"), 0, 1);
        runtime->runtime->shipEntry(2, ReplOpType::PUT, bytesOf("threshold-2"), bytesOf("value-2"), 0, 1);
        std::this_thread::sleep_for(std::chrono::milliseconds{1250});
        AKK_CLUSTER_CHECK(runtime->runtime->raftStats().snapshotIndex == 0);
        AKK_CLUSTER_CHECK(runtime->snapshotsExported.load() == 0);

        runtime->runtime->shipEntry(3, ReplOpType::PUT, bytesOf("threshold-3"), bytesOf("value-3"), 0, 1);
        AKK_CLUSTER_CHECK(waitUntil(
            [&] { return runtime->runtime->raftStats().snapshotIndex > 0 && runtime->snapshotsExported.load() == 1; },
            std::chrono::milliseconds{4000}
        ));
        runtime->runtime->close();
    }

    void testBlobReadPinDefersGcDeletion() {
        const auto dir = makeTempDir("blob-read-pin");
        auto manager = blob::BlobManager::create(dir, {});
        manager->start();
        manager->write(7, bytesOf("snapshot-blob"));
        const auto path = manager->blobPath(7);
        AKK_CLUSTER_CHECK(std::filesystem::exists(path));

        auto pin = manager->pinReads();
        manager->scheduleDelete(7);
        std::this_thread::sleep_for(std::chrono::milliseconds{300});
        AKK_CLUSTER_CHECK(std::filesystem::exists(path));

        pin = {};
        AKK_CLUSTER_CHECK(waitUntil([&] { return !std::filesystem::exists(path); }, std::chrono::milliseconds{2000}));
        manager->close();
    }

    void testRaftSnapshotInstallForCompactedFollower() {
        const auto dir = makeTempDir("raft-snapshot-install");
        const ClusterConfig cfg{
            {
                node(1, 20131, 20231),
                node(2, 20132, 20232, static_cast<uint32_t>(NodeCapability::DATA_BEARING)),
                node(3, 20133, 20233),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 2000},
        };

        ClusterRuntimeOptions options;
        options.raftBlobChunkSizeBytes = 5;
        options.raftSnapshot.minLogEntries = 1;
        auto n1 = makeRuntime(dir / "n1", cfg, 1, options);
        auto n2 = makeRuntime(dir / "n2", cfg, 2, options);
        auto n3 = makeRuntime(dir / "n3", cfg, 3, options);

        n1->runtime->start();
        n2->runtime->start();
        n3->runtime->start();

        std::array<RuntimeHarness*, 3> nodes{n1.get(), n2.get(), n3.get()};
        ensurePreferredLeader(nodes, std::chrono::milliseconds{20000});

        RuntimeHarness* leader = findLeader(nodes);
        AKK_CLUSTER_CHECK(leader != nullptr);

        const std::string key1 = "snapshot-key-1";
        const std::string value1 = "snapshot-value-1";
        runOnLeader(
            nodes,
            [&](RuntimeHarness& currentLeader) {
                leader = &currentLeader;
                currentLeader.runtime->shipEntry(1, ReplOpType::PUT, bytesOf(key1), bytesOf(value1), 0, currentLeader.nodeId);
                currentLeader.setState(1, bytesOf(key1), bytesOf(value1));
            },
            std::chrono::milliseconds{8000}
        );
        RuntimeHarness* onlineFollower = leader == n1.get() ? n3.get() : n1.get();
        AKK_CLUSTER_CHECK(waitUntil([&] { return onlineFollower->hasState(1, key1, value1); }));
        AKK_CLUSTER_CHECK(waitUntil([&] { return n2->hasState(1, key1, value1); }));

        n2->runtime->close();
        n2.reset();
        std::error_code ec;
        std::filesystem::remove_all(dir / "n2", ec);
        AKK_CLUSTER_CHECK(!ec);

        RuntimeHarness* remainingNodes[] = {n1.get(), n3.get()};
        ensurePreferredLeader(remainingNodes, std::chrono::milliseconds{20000});
        leader = findLeader(remainingNodes);
        AKK_CLUSTER_CHECK(leader != nullptr);
        leader->setState(1, bytesOf(key1), bytesOf(value1));
        const auto leaderLogPath = dir / ("n" + std::to_string(leader->nodeId)) / "cluster-raft.log";
        AKK_CLUSTER_CHECK(waitUntil([&] { return readRaftLogLastIncludedIndex(leaderLogPath) > 0; }));

        n2 = makeRuntime(dir / "n2", cfg, 2, options);
        n2->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return n2->runtime->role() != NodeRole::PRIMARY; }, std::chrono::milliseconds{1000}));

        const std::string key2 = "snapshot-key-2";
        const std::string value2 = "snapshot-value-2";
        runOnLeader(
            remainingNodes,
            [&](RuntimeHarness& currentLeader) {
                currentLeader.runtime->shipEntry(2, ReplOpType::PUT, bytesOf(key2), bytesOf(value2), 0, currentLeader.nodeId);
            },
            std::chrono::milliseconds{8000}
        );

        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                // Async replication may install a newer snapshot while catching up.
                return n2->snapshotsInstalled.load() >= 1 && n2->hasState(2, key2, value2);
            },
            std::chrono::milliseconds{8000}
        ));

        n3->runtime->close();
        n2->runtime->close();
        n1->runtime->close();
    }

    void testRaftLeaderTransfer() {
        const auto dir = makeTempDir("raft-leader-transfer");
        const ClusterConfig cfg{
            {
                node(1, 20161, 20261),
                node(2, 20162, 20262),
                node(3, 20163, 20263),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 500},
        };

        auto n1 = makeRuntime(dir / "n1", cfg, 1);
        auto n2 = makeRuntime(dir / "n2", cfg, 2);
        auto n3 = makeRuntime(dir / "n3", cfg, 3);

        n1->runtime->start();
        n2->runtime->start();
        n3->runtime->start();

        std::array<RuntimeHarness*, 3> nodes{n1.get(), n2.get(), n3.get()};
        ensurePreferredLeader(nodes, std::chrono::milliseconds{20000});

        RuntimeHarness* target = nullptr;
        runOnLeader(
            nodes,
            [&](RuntimeHarness& currentLeader) {
                for (auto* n : nodes) {
                    if (n != &currentLeader) {
                        target = n;
                        break;
                    }
                }
                AKK_CLUSTER_CHECK(target != nullptr);
                currentLeader.runtime->transferRaftLeadership(target->nodeId);
            },
            std::chrono::milliseconds{8000}
        );
        AKK_CLUSTER_CHECK(waitUntil([&] { return target->runtime->role() == NodeRole::PRIMARY; }, std::chrono::milliseconds{8000}));

        const std::string key = "transfer-key";
        const std::string value = "transfer-value";
        RuntimeHarness* writer = nullptr;
        runOnLeader(
            nodes,
            [&](RuntimeHarness& currentLeader) {
                writer = &currentLeader;
                currentLeader.runtime->shipEntry(1, ReplOpType::PUT, bytesOf(key), bytesOf(value), 0, currentLeader.nodeId);
            },
            std::chrono::milliseconds{8000}
        );
        AKK_CLUSTER_CHECK(writer != nullptr);
        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                size_t applied = 0;
                for (const auto* n : nodes) {
                    if (n != writer && n->hasState(1, key, value)) { ++applied; }
                }
                return applied == 2;
            },
            std::chrono::milliseconds{20000}
        ));

        n3->runtime->close();
        n2->runtime->close();
        n1->runtime->close();
    }

    void testRaftOnlineMembershipChange() {
        const auto dir = makeTempDir("raft-online-membership");
        const auto n1Info = node(1, 20141, 20241);
        const auto n2Info = node(2, 20142, 20242);
        const auto n3Info = node(3, 20143, 20243);
        const auto n4Info = node(4, 20144, 20244, static_cast<uint32_t>(NodeCapability::DATA_BEARING));
        const ClusterConfig initialCfg{
            {n1Info, n2Info, n3Info},
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 500},
            onlineRaftMembership(),
        };
        const ClusterConfig joiningCfg{
            {n1Info, n2Info, n3Info, n4Info},
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 500},
            onlineRaftMembership(),
            {},
            0,
            initialCfg.clusterId(),
        };

        ClusterRuntimeOptions options;
        options.raftSnapshot.minLogEntries = 1;
        auto n1 = makeRuntime(dir / "n1", initialCfg, 1, options);
        auto n2 = makeRuntime(dir / "n2", initialCfg, 2, options);
        auto n3 = makeRuntime(dir / "n3", initialCfg, 3, options);
        auto n4 = makeRuntime(dir / "n4", joiningCfg, 4, options);

        n1->runtime->start();
        n2->runtime->start();
        n3->runtime->start();
        n4->runtime->start();

        std::array<RuntimeHarness*, 3> initialNodes{n1.get(), n2.get(), n3.get()};
        ensurePreferredLeader(initialNodes, std::chrono::milliseconds{20000});

        RuntimeHarness* leader = findLeader(initialNodes);
        AKK_CLUSTER_CHECK(leader != nullptr);
        AKK_CLUSTER_CHECK(waitUntil([&] { return n4->runtime->role() != NodeRole::PRIMARY; }, std::chrono::milliseconds{1000}));
        AKK_CLUSTER_CHECK(waitUntil([&] { return findLeader(initialNodes) != nullptr; }, std::chrono::milliseconds{8000}));
        leader = findLeader(initialNodes);
        AKK_CLUSTER_CHECK(leader != nullptr);
        AKK_CLUSTER_CHECK((nodeIdsOf(leader->runtime->activeNodes()) == std::vector<uint64_t>{1, 2, 3}));

        runOnLeader(
            initialNodes,
            [&](RuntimeHarness& currentLeader) {
                try {
                    currentLeader.runtime->addRaftVotingNode(n4Info);
                }
                catch (const std::invalid_argument& error) {
                    if (std::string{error.what()}.find("node is already a voting member") == std::string::npos) { throw; }
                }
            }
        );
        ensurePreferredLeader(initialNodes, std::chrono::milliseconds{20000});
        leader = findLeader(initialNodes);
        AKK_CLUSTER_CHECK(leader != nullptr);
        AKK_CLUSTER_CHECK((nodeIdsOf(leader->runtime->activeNodes()) == std::vector<uint64_t>{1, 2, 3, 4}));

        const std::string key1 = "membership-key-1";
        const std::string value1 = "membership-value-1";
        runOnLeader(
            initialNodes,
            [&](RuntimeHarness& currentLeader) {
                currentLeader.runtime->shipEntry(1, ReplOpType::PUT, bytesOf(key1), bytesOf(value1), 0, currentLeader.nodeId);
            }
        );
        AKK_CLUSTER_CHECK(waitUntil([&] { return n4->hasState(1, key1, value1); }, std::chrono::milliseconds{8000}));

        ensurePreferredLeader(initialNodes, std::chrono::milliseconds{20000});
        leader = findLeader(initialNodes);
        AKK_CLUSTER_CHECK(leader != nullptr);
        runOnLeader(
            initialNodes,
            [&](RuntimeHarness& currentLeader) {
                try {
                    currentLeader.runtime->removeRaftVotingNode(4);
                }
                catch (const std::invalid_argument& error) {
                    if (std::string{error.what()}.find("node is not a voting member") == std::string::npos) { throw; }
                }
            }
        );
        ensurePreferredLeader(initialNodes, std::chrono::milliseconds{20000});
        leader = findLeader(initialNodes);
        AKK_CLUSTER_CHECK(leader != nullptr);
        AKK_CLUSTER_CHECK((nodeIdsOf(leader->runtime->activeNodes()) == std::vector<uint64_t>{1, 2, 3}));

        const std::string key2 = "membership-key-2";
        const std::string value2 = "membership-value-2";
        runOnLeader(
            initialNodes,
            [&](RuntimeHarness& currentLeader) {
                currentLeader.runtime->shipEntry(2, ReplOpType::PUT, bytesOf(key2), bytesOf(value2), 0, currentLeader.nodeId);
            }
        );
        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                size_t applied = 0;
                for (const auto* n : initialNodes) {
                    if (n->hasState(2, key2, value2)) { ++applied; }
                }
                return applied >= 1;
            },
            std::chrono::milliseconds{20000}
        ));
        AKK_CLUSTER_CHECK(waitUntil([&] { return !n4->hasState(2, key2, value2); }, std::chrono::milliseconds{1000}));
        AKK_CLUSTER_CHECK(!n4->hasState(2, key2, value2));

        AKK_CLUSTER_CHECK(waitUntil([&] {
            return readRaftLogLastIncludedIndex(dir / "n1" / "cluster-raft.log") > 0 ||
                readRaftLogLastIncludedIndex(dir / "n2" / "cluster-raft.log") > 0 ||
                readRaftLogLastIncludedIndex(dir / "n3" / "cluster-raft.log") > 0;
        }));

        n4->runtime->close();
        n3->runtime->close();
        n2->runtime->close();
        n1->runtime->close();

        n1 = makeRuntime(dir / "n1", initialCfg, 1, options);
        n2 = makeRuntime(dir / "n2", initialCfg, 2, options);
        n3 = makeRuntime(dir / "n3", initialCfg, 3, options);
        n1->runtime->start();
        n2->runtime->start();
        n3->runtime->start();
        initialNodes = {n1.get(), n2.get(), n3.get()};
        ensurePreferredLeader(initialNodes, std::chrono::milliseconds{20000});
        leader = findLeader(initialNodes);
        AKK_CLUSTER_CHECK(leader != nullptr);
        AKK_CLUSTER_CHECK((nodeIdsOf(leader->runtime->activeNodes()) == std::vector<uint64_t>{1, 2, 3}));

        n3->runtime->close();
        n2->runtime->close();
        n1->runtime->close();
    }

    void testParallelReplicationHandshakes(bool secure) {
        constexpr uint16_t port = 23001;
        const auto dir = makeTempDir(secure ? "parallel-secure-handshake" : "parallel-plain-handshake");
        ClusterRuntimeOptions options;
        options.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
        if (secure) {
            for (uint64_t id : {1, 2}) {
                const auto identity = akkaradb::crypto::IdentityStore{dir / std::to_string(id)}.loadOrCreate();
                options.secure.pinnedPeers.push_back({id, identity.publicKey});
            }
            options.secure.identitySeedPath = dir / "1";
        }
        auto server = ReplicationServer::create(port, 1, [] { return uint64_t{0}; }, {}, {}, 1, {2}, options);
        server->start();
        struct SilentSocket {
#ifdef _WIN32
            SOCKET socket = INVALID_SOCKET;
            ~SilentSocket() { if (socket != INVALID_SOCKET) { ::closesocket(socket); } }
#else
            int socket = -1;
            ~SilentSocket() { if (socket >= 0) { ::close(socket); } }
#endif
            explicit SilentSocket(uint16_t port) {
                socket = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
                sockaddr_in address{};
                address.sin_family = AF_INET;
                address.sin_port = htons(port);
                address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
                const int result = ::connect(socket, reinterpret_cast<sockaddr*>(&address), sizeof(address));
                if (result != 0) {
#ifdef _WIN32
                    ::closesocket(socket);
#else
                    ::close(socket);
#endif
                    throw std::runtime_error("silent test connection failed");
                }
            }
        };
        std::vector<std::unique_ptr<SilentSocket>> silent;
        for (unsigned i = 0; i < 3; ++i) { silent.push_back(std::make_unique<SilentSocket>(port)); }
        options.secure.identitySeedPath = dir / "2";
        options.secure.expectedPrimaryNodeId = 1;
        auto client = ReplicationClient::create("127.0.0.1", port, 2, [] { return uint64_t{0}; }, {}, options);
        client->start();
        // Three peers never send even their first hello; a fourth must still connect.
        AKK_CLUSTER_CHECK(waitUntil([&] { return client->connected(); }, std::chrono::milliseconds{2000}));
        auto duplicate = ReplicationClient::create("127.0.0.1", port, 2, [] { return uint64_t{0}; }, {}, options);
        duplicate->start();
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
        AKK_CLUSTER_CHECK(server->replicaCount() == 1 && client->connected());
        duplicate->close();
        client->close();
        for (unsigned i = 0; i < 100; ++i) { silent.push_back(std::make_unique<SilentSocket>(port)); }
        const auto beforeClose = std::chrono::steady_clock::now();
        server->close();
        AKK_CLUSTER_CHECK(std::chrono::steady_clock::now() - beforeClose < std::chrono::seconds{2});
        AKK_CLUSTER_CHECK(server->replicaCount() == 0);
        silent.clear();
        server->start();
        client->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return client->connected(); }));
        client->close();
        server->close();
    }

    void testClusterEndpointAndPinValidation() {
        auto rejects = [](auto&& operation) {
            bool rejected = false;
            try { operation(); }
            catch (const std::invalid_argument&) { rejected = true; }
            AKK_CLUSTER_CHECK(rejected);
        };
        rejects([] { (void)ClusterConfig{{node(1, 1, 0), node(2, 2, 3)}, ReplicationMode::MIRROR, {}}; });
        rejects([] { (void)ClusterConfig{{node(1, 1, 3), node(2, 2, 3)}, ReplicationMode::MIRROR, {}}; });
        rejects([] {
            auto a = node(1, 1, 3), b = node(2, 2, 3);
            a.host = "DB.example";
            b.host = "db.example.";
            (void)ClusterConfig{{a, b}, ReplicationMode::MIRROR, {}};
        });
        rejects([] {
            auto a = node(1, 1, 3);
            a.host = std::string{"bad\0host", 8};
            (void)ClusterConfig{{a}, ReplicationMode::MIRROR, {}};
        });
        rejects([] {
            (void)ClusterConfig{{node(1, 0, 0)}, ReplicationMode::STANDALONE, {},
                               ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM}};
        });
        (void)ClusterConfig{{node(1, 0, 0)}, ReplicationMode::STANDALONE, {}};
        const ClusterConfig mirror{{node(1, 1, 3), node(2, 2, 4), node(3, 5, 6)}, ReplicationMode::MIRROR, {}};
        ClusterRuntimeOptions options;
        rejects([&] { mirror.validateRuntime(1, options); });
        options.secure.pinnedPeers = {{1, {}}};
        mirror.validateRuntime(2, options); // A MIRROR replica only needs the fixed Primary.
        rejects([&] { mirror.validateRuntime(1, options); });
        options.secure.pinnedPeers = {{1, {}}, {2, {}}, {3, {}}, {99, {}}};
        mirror.validateRuntime(1, options); // Future-member pins are allowed.
        options.secure.pinnedPeers.push_back({2, {}});
        rejects([&] { mirror.validateRuntime(1, options); });
        options.secure.pinnedPeers = {{0, {}}};
        rejects([&] { mirror.validateRuntime(1, options); });
        options.transportMode = TransportMode::PLAIN;
        options.secure.pinnedPeers.clear();
        mirror.validateRuntime(1, options);
        rejects([&] { mirror.validateRuntime(99, options); });
        const ClusterConfig raft{mirror.nodes(), ReplicationMode::MIRROR, {},
                                 ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM}};
        options.transportMode = TransportMode::SECURE;
        options.secure.pinnedPeers = {{1, {}}};
        rejects([&] { raft.validateRuntime(2, options); });
        const auto dir = makeTempDir("engine-pin-validation-before-storage");
        auto engineOptions = stripeStorageOptions(dir, 1);
        engineOptions.cluster.runtime.transportMode = TransportMode::SECURE;
        rejects([&] { (void)akkaradb::engine::AkkEngine::open(engineOptions); });
        AKK_CLUSTER_CHECK(!std::filesystem::exists(engineOptions.paths.dataDir / "cluster.identity"));
        AKK_CLUSTER_CHECK(!std::filesystem::exists(engineOptions.paths.dataDir / "cluster.membership"));
        const ClusterConfig singleRaft{{node(1, 23011, 23012)}, ReplicationMode::MIRROR, {},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM}, onlineRaftMembership()};
        auto runtime = makeRuntime(dir / "secure-raft", singleRaft, 1, {}, true);
        runtime->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return runtime->runtime->role() == NodeRole::PRIMARY; }));
        rejects([&] { runtime->runtime->addRaftVotingNode(node(2, 23013, 23014)); });
        rejects([&] { runtime->runtime->addRaftVotingNode(node(2, 23013, 23012)); });
        rejects([&] { runtime->runtime->addRaftVotingNode(node(2, 23013, 0)); });
        AKK_CLUSTER_CHECK(runtime->runtime->role() == NodeRole::PRIMARY);
        runtime->runtime->close();
    }

    void testStripeReadRepairStatistics() {
        const auto dir = makeTempDir("stripe-repair-statistics");
        const auto key = keyOwnedBy(stripeStorageConfig(), 1, "repair-stats");
        auto n1 = akkaradb::engine::AkkEngine::open(stripeStorageOptions(dir, 1));
        auto n2 = akkaradb::engine::AkkEngine::open(stripeStorageOptions(dir, 2));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { n1->put(bytesOf(key), bytesOf("repair-value")); return true; }
            catch (const std::runtime_error&) { return false; }
        }));
        AKK_CLUSTER_CHECK(n1->stats().stripeReadRepair.attempts == 0);
        n2->close();
        n2.reset();
        const auto value = engineGet(*n1, key);
        AKK_CLUSTER_CHECK(value && textOf(*value) == "repair-value");
        const auto failed = n1->stats().stripeReadRepair;
        AKK_CLUSTER_CHECK(failed.attempts == 1 && failed.failed == 1 && failed.succeeded == 0 && failed.lastFailureNodeId == 2);
        // Remove the replica's shard through its offline local storage, leaving
        // the owner's published metadata untouched.
        auto offlineOptions = stripeStorageOptions(dir, 2);
        offlineOptions.components.clusterEnabled = false;
        auto offline = akkaradb::engine::AkkEngine::open(offlineOptions);
        akkaradb::core::BufferArena arena;
        std::vector<std::vector<uint8_t>> shardKeys;
        for (const auto& record : offline->scan(arena)) {
            if (record.key.size() >= 6 && record.key[0] == 0 && record.key[4] == 'S') {
                shardKeys.emplace_back(record.key.begin(), record.key.end());
            }
        }
        AKK_CLUSTER_CHECK(!shardKeys.empty());
        for (const auto& shardKey : shardKeys) { offline->remove(shardKey); }
        offline->close();
        n2 = akkaradb::engine::AkkEngine::open(stripeStorageOptions(dir, 2));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto repaired = engineGet(*n1, key);
            return repaired && textOf(*repaired) == "repair-value" && n1->stats().stripeReadRepair.succeeded >= 1;
        }));
        const auto repaired = n1->stats().stripeReadRepair;
        AKK_CLUSTER_CHECK(repaired.attempts == repaired.succeeded + repaired.failed);
        AKK_CLUSTER_CHECK(repaired.failed >= 1 && repaired.lastFailureNodeId == 2);
        AKK_CLUSTER_CHECK(stripeInternalCounts(*n2).first == 1);
        const auto again = engineGet(*n1, key);
        AKK_CLUSTER_CHECK(again && n1->stats().stripeReadRepair.attempts == repaired.attempts);
        AKK_CLUSTER_CHECK(n2->stats().stripeReadRepair.attempts == 0);
        n2->close();
        n1->close();
        auto disabledOptions = stripeStorageOptions(dir, 1);
        disabledOptions.cluster.runtime.stripeReadRepair = false;
        n1 = akkaradb::engine::AkkEngine::open(disabledOptions);
        const auto withoutRepair = engineGet(*n1, key);
        AKK_CLUSTER_CHECK(withoutRepair && textOf(*withoutRepair) == "repair-value");
        AKK_CLUSTER_CHECK(n1->stats().stripeReadRepair.attempts == 0);
        n1->close();
    }

    void testPrimaryAckPathStillUsesLegacyRuntime() {
        constexpr uint16_t port = 20211;
        ClusterRuntimeOptions options;
        options.transportMode = TransportMode::PLAIN;

        const AckPolicy all{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::APPLIED};
        auto server = ReplicationServer::create(port, 1, [] { return uint64_t{1}; }, all, {}, 0, {}, options);
        std::atomic<uint64_t> lastSeq{0};
        std::atomic<int> applied{0};

        auto client = ReplicationClient::create("127.0.0.1", port, 2, [&] { return lastSeq.load(); }, all, options);
        client->setApplyCallback(
            [&](uint64_t seq, ReplOpType op, std::span<const uint8_t> key, std::span<const uint8_t> value, uint8_t flags, uint64_t source) {
                AKK_CLUSTER_CHECK(seq == 1);
                AKK_CLUSTER_CHECK(op == ReplOpType::PUT);
                AKK_CLUSTER_CHECK(textOf(key) == "legacy-key");
                AKK_CLUSTER_CHECK(textOf(value) == "legacy-value");
                AKK_CLUSTER_CHECK(flags == 3);
                AKK_CLUSTER_CHECK(source == 1);
                lastSeq.store(seq);
                applied.fetch_add(1);
            }
        );

        server->start();
        client->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return server->replicaCount() == 1; }));

        const std::string key = "legacy-key";
        const std::string value = "legacy-value";
        server->shipEntry(1, ReplOpType::PUT, bytesOf(key), bytesOf(value), 3, 1);
        AKK_CLUSTER_CHECK(waitUntil([&] { return applied.load() == 1; }));

        client->close();
        server->close();
    }

    void testReplicationServerStreamsSnapshotCatchupThroughBoundedQueue() {
        constexpr uint16_t port = 20216;
        ClusterRuntimeOptions options;
        options.transportMode = TransportMode::PLAIN;
        options.maxReplicaQueueFrames = 2;
        options.maxReplicaQueueBytes = 1024 * 1024;

        const AckPolicy ack{};
        ReplicationServer::Snapshot snapshot;
        snapshot.seq = 8;
        auto snapshotEntries = std::make_shared<std::vector<ReplSnapshotEntry>>();
        for (uint8_t i = 0; i < 8; ++i) {
            snapshotEntries->push_back(ReplSnapshotEntry{
                .key = std::vector<uint8_t>{'k', i},
                .value = std::vector<uint8_t>(256, static_cast<uint8_t>(0x50 + i)),
            });
        }
        snapshot.entryCount = snapshotEntries->size();
        snapshot.forEachEntry = [snapshotEntries](const ReplicationServer::Snapshot::EntryVisitor& visitor) {
            for (const auto& entry : *snapshotEntries) {
                if (!visitor(entry.key, entry.value)) { return false; }
            }
            return true;
        };

        auto server = ReplicationServer::create(
            port,
            1,
            [] { return uint64_t{8}; },
            ack,
            {},
            1,
            {2},
            options,
            [](uint64_t, uint64_t) -> std::optional<std::vector<ReplEntry>> { return std::nullopt; },
            [snapshot] { return std::optional<ReplicationServer::Snapshot>{snapshot}; }
        );
        std::atomic<uint64_t> lastSeq{0};
        std::atomic<uint64_t> entryCount{0};
        auto client = ReplicationClient::create("127.0.0.1", port, 2, [&] { return lastSeq.load(); }, ack, options);
        client->setSnapshotCallbacks(
            [&](uint64_t seq, uint64_t count) {
                AKK_CLUSTER_CHECK(seq == 8);
                AKK_CLUSTER_CHECK(count == 8);
            },
            [](std::span<const uint8_t> key, uint64_t valueSize, uint32_t) {
                AKK_CLUSTER_CHECK(key.size() == 2 && key[0] == 'k');
                AKK_CLUSTER_CHECK(valueSize == 256);
            },
            [](uint64_t offset, std::span<const uint8_t> chunk) {
                AKK_CLUSTER_CHECK(offset == 0);
                AKK_CLUSTER_CHECK(chunk.size() == 256);
            },
            [&] { entryCount.fetch_add(1); },
            [&](uint64_t seq) { lastSeq.store(seq); }
        );

        server->start();
        client->start();
        AKK_CLUSTER_CHECK(waitUntil([&] {
            return server->replicaCount() == 1 && lastSeq.load() == 8 && entryCount.load() == 8;
        }));

        client->close();
        server->close();
    }

    void testReplicationServerRejectsOversizedCatchupFrame() {
        constexpr uint16_t port = 20218;
        ClusterRuntimeOptions options;
        options.transportMode = TransportMode::PLAIN;
        options.maxReplicaQueueFrames = 2;
        options.maxReplicaQueueBytes = 1;

        const AckPolicy ack{};
        ReplicationServer::Snapshot snapshot;
        snapshot.seq = 1;
        auto snapshotEntry = std::make_shared<ReplSnapshotEntry>(ReplSnapshotEntry{
            .key = std::vector<uint8_t>{'k'},
            .value = std::vector<uint8_t>{'v'},
        });
        snapshot.entryCount = 1;
        snapshot.forEachEntry = [snapshotEntry](const ReplicationServer::Snapshot::EntryVisitor& visitor) {
            return visitor(snapshotEntry->key, snapshotEntry->value);
        };
        auto server = ReplicationServer::create(
            port,
            1,
            [] { return uint64_t{1}; },
            ack,
            {},
            1,
            {2},
            options,
            [](uint64_t, uint64_t) -> std::optional<std::vector<ReplEntry>> { return std::nullopt; },
            [snapshot] { return std::optional<ReplicationServer::Snapshot>{snapshot}; }
        );
        auto client = ReplicationClient::create("127.0.0.1", port, 2, [] { return uint64_t{0}; }, ack, options);

        server->start();
        client->start();
        std::this_thread::sleep_for(std::chrono::milliseconds{400});
        AKK_CLUSTER_CHECK(server->replicaCount() == 0);

        client->close();
        server->close();
    }

    void testReplicationServerOrdersWritesCommittedDuringCatchup() {
        constexpr uint16_t port = 20219;
        ClusterRuntimeOptions options;
        options.transportMode = TransportMode::PLAIN;
        options.maxReplicaQueueFrames = 2;
        options.maxReplicaQueueBytes = 1024 * 1024;

        std::mutex historyMutex;
        std::vector<ReplEntry> history;
        for (uint64_t seq = 1; seq <= 64; ++seq) {
            history.push_back(ReplEntry{
                .seq = seq,
                .sourceNodeId = 1,
                .op = ReplOpType::PUT,
                .key = std::vector<uint8_t>{static_cast<uint8_t>(seq)},
                .value = std::vector<uint8_t>{static_cast<uint8_t>(seq + 1)},
            });
        }
        std::atomic<uint64_t> currentSeq{64};
        std::mutex gateMutex;
        std::condition_variable gateCv;
        bool initialHistoryRequested = false;
        bool releaseInitialHistory = false;

        const AckPolicy ack{};
        auto server = ReplicationServer::create(
            port,
            1,
            [&] { return currentSeq.load(); },
            ack,
            {},
            1,
            {2},
            options,
            [&](uint64_t afterSeq, uint64_t throughSeq) -> std::optional<std::vector<ReplEntry>> {
                if (afterSeq == 0) {
                    std::unique_lock lock{gateMutex};
                    initialHistoryRequested = true;
                    gateCv.notify_one();
                    gateCv.wait(lock, [&] { return releaseInitialHistory; });
                }
                std::lock_guard lock{historyMutex};
                std::vector<ReplEntry> out;
                for (const auto& entry : history) {
                    if (entry.seq > afterSeq && entry.seq <= throughSeq) { out.push_back(entry); }
                }
                return out;
            }
        );
        std::atomic<uint64_t> appliedSeq{0};
        auto client = ReplicationClient::create("127.0.0.1", port, 2, [&] { return appliedSeq.load(); }, ack, options);
        client->setApplyCallback(
            [&](uint64_t seq, ReplOpType, std::span<const uint8_t>, std::span<const uint8_t>, uint8_t, uint64_t) {
                AKK_CLUSTER_CHECK(seq == appliedSeq.load() + 1);
                appliedSeq.store(seq);
            }
        );

        server->start();
        client->start();
        {
            std::unique_lock lock{gateMutex};
            AKK_CLUSTER_CHECK(gateCv.wait_for(lock, std::chrono::seconds{5}, [&] { return initialHistoryRequested; }));
        }
        const ReplEntry concurrent{
            .seq = 65,
            .sourceNodeId = 1,
            .op = ReplOpType::PUT,
            .key = std::vector<uint8_t>{65},
            .value = std::vector<uint8_t>{66},
        };
        {
            std::lock_guard lock{historyMutex};
            history.push_back(concurrent);
        }
        currentSeq.store(65);
        server->shipEntry(
            concurrent.seq,
            concurrent.op,
            concurrent.key,
            concurrent.value,
            concurrent.recordFlags,
            concurrent.sourceNodeId
        );
        {
            std::lock_guard lock{gateMutex};
            releaseInitialHistory = true;
        }
        gateCv.notify_one();

        AKK_CLUSTER_CHECK(waitUntil([&] { return server->replicaCount() == 1 && appliedSeq.load() == 65; }));

        client->close();
        server->close();
    }

    void testReplicationServerReapsDisconnectedReplicas() {
        constexpr uint16_t port = 20217;
        ClusterRuntimeOptions options;
        options.transportMode = TransportMode::PLAIN;

        const AckPolicy ack{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::APPLIED};
        auto server = ReplicationServer::create(port, 1, [] { return uint64_t{1}; }, ack, {}, 1, {2}, options);
        server->start();

        for (size_t iteration = 0; iteration < 64; ++iteration) {
            auto client = ReplicationClient::create("127.0.0.1", port, 2, [] { return uint64_t{0}; }, ack, options);
            client->start();
            AKK_CLUSTER_CHECK(waitUntil([&] { return server->replicaCount() == 1; }));
            client->close();
            AKK_CLUSTER_CHECK(waitUntil([&] { return server->replicaCount() == 0; }));
        }

        std::atomic<uint64_t> appliedSeq{0};
        auto finalClient = ReplicationClient::create("127.0.0.1", port, 2, [&] { return appliedSeq.load(); }, ack, options);
        finalClient->setApplyCallback(
            [&](uint64_t seq, ReplOpType, std::span<const uint8_t>, std::span<const uint8_t>, uint8_t, uint64_t) { appliedSeq.store(seq); }
        );
        finalClient->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return server->replicaCount() == 1; }));
        server->shipEntry(1, ReplOpType::PUT, bytesOf("reap-key"), bytesOf("reap-value"), 0, 1);
        AKK_CLUSTER_CHECK(waitUntil([&] { return appliedSeq.load() == 1; }));

        finalClient->close();
        server->close();
    }

    void testAkkEngineRejectsUnsafeNativeClusterConfigurations() {
        const auto dir = makeTempDir("engine-unsafe-native-cluster");

        {
            const ClusterConfig nonRaft{
                {
                    node(1, 20368, 20468),
                    node(2, 20369, 20469),
                },
                ReplicationMode::MIRROR,
                AckPolicy{},
                ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK},
            };

            auto options = akkaradb::engine::AkkEngineOptions{};
            options.paths.dataDir = dir / "non-raft-blob";
            options.components.clusterEnabled = true;
            options.components.blobEnabled = true;
            options.cluster.config = nonRaft;
            options.cluster.runtime.transportMode = TransportMode::PLAIN;
            options.cluster.runtime.startupRole = NodeStartupRole::PRIMARY;
            writeNodeIdFile(options.paths.dataDir / "node.id", 1);

            bool rejected = false;
            try {
                auto engine = akkaradb::engine::AkkEngine::open(options);
                (void)engine;
            }
            catch (const std::invalid_argument&) {
                rejected = true;
            }
            AKK_CLUSTER_CHECK(rejected);
        }

        {
            StripeOptions stripeOptions;
            stripeOptions.dataShards = 2;
            stripeOptions.parityShards = 1;
            bool rejected = false;
            try {
                const ClusterConfig raftStripe{
                    {
                        node(1, 20370, 20470),
                        node(2, 20371, 20471),
                        node(3, 20372, 20472),
                    },
                    ReplicationMode::STRIPE,
                    AckPolicy{},
                    ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM},
                    {},
                    stripeOptions,
                };
                (void)raftStripe;
            }
            catch (const std::invalid_argument&) {
                rejected = true;
            }
            AKK_CLUSTER_CHECK(rejected);
        }

        {
            const ClusterConfig partitioned{
                {
                    node(1, 20373, 20473),
                    node(2, 20374, 20474),
                },
                ReplicationMode::PARTITIONED,
                AckPolicy{},
                ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK},
            };

            auto options = akkaradb::engine::AkkEngineOptions{};
            options.paths.dataDir = dir / "partitioned-without-wal";
            options.components.clusterEnabled = true;
            options.components.blobEnabled = false;
            options.components.walEnabled = false;
            options.cluster.config = partitioned;
            options.cluster.runtime.transportMode = TransportMode::PLAIN;
            options.cluster.runtime.clusterGroupId = 9101;
            writeNodeIdFile(options.paths.dataDir / "node.id", 1);

            bool rejected = false;
            try {
                auto engine = akkaradb::engine::AkkEngine::open(options);
                (void)engine;
            }
            catch (const std::invalid_argument&) {
                rejected = true;
            }
            AKK_CLUSTER_CHECK(rejected);
        }
    }

    void testPrimaryAckFailWriteCommitsLocalBeforeAckFailure() {
        const auto dir = makeTempDir("primary-ack-failwrite-local-first");
        const AckPolicy allApplied{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::APPLIED};
        ConsistencyOptions consistency;
        consistency.mode = ConsistencyMode::PRIMARY_ACK;
        consistency.ackTimeoutAction = AckTimeoutAction::FAIL_WRITE;
        consistency.ackTimeoutMs = 50;
        const ClusterConfig cfg{
            {
                node(1, 20373, 20473),
                node(2, 20374, 20474),
            },
            ReplicationMode::MIRROR,
            allApplied,
            consistency,
        };

        auto options = akkaradb::engine::AkkEngineOptions{};
        options.paths.dataDir = dir / "primary";
        options.components.clusterEnabled = true;
        options.components.blobEnabled = false;
        options.cluster.config = cfg;
        options.cluster.runtime.transportMode = TransportMode::PLAIN;
        options.cluster.runtime.startupRole = NodeStartupRole::PRIMARY;
        writeNodeIdFile(options.paths.dataDir / "node.id", 1);

        auto engine = akkaradb::engine::AkkEngine::open(options);
        const std::string key = "failwrite-local-first-key";
        const std::string value = "failwrite-local-first-value";
        bool rejected = false;
        try {
            engine->put(bytesOf(key), bytesOf(value));
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        AKK_CLUSTER_CHECK(rejected);
        const auto stored = engineGet(*engine, key);
        AKK_CLUSTER_CHECK(stored.has_value());
        AKK_CLUSTER_CHECK(textOf(*stored) == value);
        engine->close();
    }

    void testSecureReplicationRequiresPeerPins() {
        constexpr uint16_t port = 20215;
        const auto dir = makeTempDir("secure-requires-peer-pins");
        ClusterRuntimeOptions serverOptions;
        serverOptions.transportMode = TransportMode::SECURE;
        serverOptions.secure.identitySeedPath = dir / "server.identity";

        ClusterRuntimeOptions clientOptions;
        clientOptions.transportMode = TransportMode::SECURE;
        clientOptions.secure.identitySeedPath = dir / "client.identity";
        clientOptions.secure.expectedPrimaryNodeId = 1;

        const AckPolicy ack{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::APPLIED};
        bool serverRejected = false, clientRejected = false;
        try { (void)ReplicationServer::create(port, 1, [] { return uint64_t{0}; }, ack, {}, 1, {2}, serverOptions); }
        catch (const std::invalid_argument&) { serverRejected = true; }
        try { (void)ReplicationClient::create("127.0.0.1", port, 2, [] { return uint64_t{0}; }, ack, clientOptions); }
        catch (const std::invalid_argument&) { clientRejected = true; }
        AKK_CLUSTER_CHECK(serverRejected && clientRejected);
        AKK_CLUSTER_CHECK(!std::filesystem::exists(serverOptions.secure.identitySeedPath));
        AKK_CLUSTER_CHECK(!std::filesystem::exists(clientOptions.secure.identitySeedPath));
    }

    void testPrimaryAckAllTargetsFailsWithoutReplicas() {
        constexpr uint16_t port = 20212;
        ClusterRuntimeOptions options;
        options.transportMode = TransportMode::PLAIN;

        const AckPolicy all{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::APPLIED};
        ConsistencyOptions consistency;
        consistency.ackTimeoutAction = AckTimeoutAction::FAIL_ACK;
        consistency.ackTimeoutMs = 50;

        auto server = ReplicationServer::create(port, 1, [] { return uint64_t{1}; }, all, consistency, 1, {}, options);
        server->start();

        bool rejected = false;
        try {
            const std::string key = "missing-replica-key";
            const std::string value = "missing-replica-value";
            server->shipEntry(1, ReplOpType::PUT, bytesOf(key), bytesOf(value), 0, 1);
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        AKK_CLUSTER_CHECK(rejected);

        server->close();
    }

    void testReplicaRejectsImplicitNonRaftGroupSwitch() {
        const auto dir = makeTempDir("non-raft-membership");
        constexpr uint16_t firstPort = 20213;
        constexpr uint16_t secondPort = 20214;
        const AckPolicy none{};

        ClusterRuntimeOptions firstOptions;
        firstOptions.transportMode = TransportMode::PLAIN;
        firstOptions.clusterGroupId = 0xAABBCCDD;
        firstOptions.clusterGroupEpoch = 1;

        ClusterRuntimeOptions replicaOptions = firstOptions;
        replicaOptions.clusterMembershipPath = dir / "cluster.membership";

        auto first = ReplicationServer::create(
            firstPort,
            1,
            [] { return uint64_t{0}; },
            none,
            {},
            1,
            {2},
            firstOptions
        );
        auto replica = ReplicationClient::create("127.0.0.1", firstPort, 2, [] { return uint64_t{0}; }, none, replicaOptions);
        first->start();
        replica->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return first->replicaCount() == 1; }));
        replica->close();
        first->close();

        ClusterRuntimeOptions secondOptions = firstOptions;
        auto second = ReplicationServer::create(
            secondPort,
            3,
            [] { return uint64_t{0}; },
            none,
            {},
            1,
            {2},
            secondOptions
        );
        auto rejectedReplica = ReplicationClient::create("127.0.0.1", secondPort, 2, [] { return uint64_t{0}; }, none, replicaOptions);
        second->start();
        rejectedReplica->start();
        std::this_thread::sleep_for(std::chrono::milliseconds{400});
        AKK_CLUSTER_CHECK(second->replicaCount() == 0);
        rejectedReplica->close();

        replicaOptions.resetClusterMembership = true;
        auto resetReplica = ReplicationClient::create("127.0.0.1", secondPort, 2, [] { return uint64_t{0}; }, none, replicaOptions);
        resetReplica->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return second->replicaCount() == 1; }));
        resetReplica->close();
        second->close();
    }

    void testNonRaftMirrorRejectsSecondPrimary() {
        const auto dir = makeTempDir("non-raft-single-primary");
        const ClusterConfig cfg{
            {
                node(1, 20181, 20281),
                node(2, 20182, 20282),
                node(3, 20183, 20283),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK},
        };
        AKK_CLUSTER_CHECK(cfg.primaryNodeId() == 1);

        ClusterRuntimeOptions n1Options;
        n1Options.transportMode = TransportMode::PLAIN;
        n1Options.startupRole = NodeStartupRole::PRIMARY;
        auto n1 = makeRuntime(dir / "n1", cfg, 1, n1Options);
        n1->runtime->start();
        AKK_CLUSTER_CHECK(n1->runtime->role() == NodeRole::PRIMARY);

        ClusterRuntimeOptions n3Options;
        n3Options.transportMode = TransportMode::PLAIN;
        n3Options.startupRole = NodeStartupRole::PRIMARY;
        auto n3 = makeRuntime(dir / "n3", cfg, 3, n3Options);
        bool rejectedSecondPrimary = false;
        try {
            n3->runtime->start();
        }
        catch (const std::runtime_error&) {
            rejectedSecondPrimary = true;
        }
        AKK_CLUSTER_CHECK(rejectedSecondPrimary);

        const auto group1 = readGroupState(dir / "n1" / "cluster.membership");
        AKK_CLUSTER_CHECK(group1.groupId != 0);
        AKK_CLUSTER_CHECK(group1.primaryNodeId == 1);
        AKK_CLUSTER_CHECK(group1.groupEpoch == 1);
        AKK_CLUSTER_CHECK(!std::filesystem::exists(dir / "n3" / "cluster.membership"));

        n3->runtime->close();
        n1->runtime->close();
    }

    void testNonRaftPrimaryRejectsCorruptGroupState() {
        const auto dir = makeTempDir("non-raft-corrupt-group-state");
        writeTextFile(dir / "n1" / "cluster.membership", "not a valid AKCG2 state\n");

        const ClusterConfig cfg{
            {
                node(1, 20321, 20421),
                node(2, 20322, 20422),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK},
        };

        ClusterRuntimeOptions options;
        options.startupRole = NodeStartupRole::PRIMARY;
        auto primary = makeRuntime(dir / "n1", cfg, 1, options);

        bool rejected = false;
        try {
            primary->runtime->start();
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        AKK_CLUSTER_CHECK(rejected);
        primary->runtime->close();
    }

    void testNonRaftPrimaryBacksUpCorruptGroupState() {
        const auto dir = makeTempDir("non-raft-corrupt-group-state-backup");
        const auto membershipPath = dir / "n1" / "cluster.membership";
        writeTextFile(membershipPath, "not a valid AKCG2 state\n");

        const ClusterConfig cfg{
            {
                node(1, 20331, 20431),
                node(2, 20332, 20432),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK},
        };

        ClusterRuntimeOptions options;
        options.startupRole = NodeStartupRole::PRIMARY;
        options.corruptStateAction = CorruptClusterStateAction::BACKUP_AND_RECREATE;
        auto primary = makeRuntime(dir / "n1", cfg, 1, options);
        primary->runtime->start();
        AKK_CLUSTER_CHECK(primary->runtime->role() == NodeRole::PRIMARY);
        AKK_CLUSTER_CHECK(std::filesystem::exists(membershipPath.string() + ".corrupt"));
        const auto state = readGroupState(membershipPath);
        AKK_CLUSTER_CHECK(state.groupId != 0);
        AKK_CLUSTER_CHECK(state.primaryNodeId == 1);
        AKK_CLUSTER_CHECK(state.groupEpoch == 1);
        primary->runtime->close();
    }

    void testNonRaftPrimaryReusesGroupStateAfterRestart() {
        const auto dir = makeTempDir("non-raft-group-restart");
        const ClusterConfig cfg{
            {
                node(1, 20351, 20451),
                node(2, 20352, 20452),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK},
        };

        ClusterRuntimeOptions options;
        options.startupRole = NodeStartupRole::PRIMARY;
        auto first = makeRuntime(dir / "n1", cfg, 1, options);
        first->runtime->start();
        AKK_CLUSTER_CHECK(first->runtime->role() == NodeRole::PRIMARY);
        const auto initial = readGroupState(dir / "n1" / "cluster.membership");
        first->runtime->close();
        first.reset();

        auto restarted = makeRuntime(dir / "n1", cfg, 1, options);
        restarted->runtime->start();
        AKK_CLUSTER_CHECK(restarted->runtime->role() == NodeRole::PRIMARY);
        const auto recovered = readGroupState(dir / "n1" / "cluster.membership");
        AKK_CLUSTER_CHECK(recovered.groupId == initial.groupId);
        AKK_CLUSTER_CHECK(recovered.primaryNodeId == initial.primaryNodeId);
        AKK_CLUSTER_CHECK(recovered.groupEpoch == initial.groupEpoch);
        restarted->runtime->close();
    }

    void testClusterRuntimeRejectsUnknownReplicaFromAckQuorum() {
        const auto dir = makeTempDir("cluster-runtime-unknown-replica");
        ConsistencyOptions consistency;
        consistency.mode = ConsistencyMode::PRIMARY_ACK;
        consistency.writeConsistency = WriteConsistency::ALL_CONFIGURED;
        consistency.ackTimeoutAction = AckTimeoutAction::FAIL_WRITE;
        consistency.ackTimeoutMs = 100;

        const ClusterConfig primaryCfg{
            {
                node(1, 20331, 20431),
                node(2, 20332, 20432),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            consistency,
        };
        const ClusterConfig unknownReplicaCfg{
            {
                node(1, 20331, 20431),
                node(3, 20333, 20433),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            consistency,
        };

        ClusterRuntimeOptions primaryOptions;
        primaryOptions.startupRole = NodeStartupRole::PRIMARY;
        auto primary = makeRuntime(dir / "n1", primaryCfg, 1, primaryOptions);
        primary->runtime->start();
        AKK_CLUSTER_CHECK(primary->runtime->role() == NodeRole::PRIMARY);

        ClusterRuntimeOptions unknownOptions;
        unknownOptions.startupRole = NodeStartupRole::REPLICA;
        unknownOptions.primaryNodeId = 1;
        auto unknownReplica = makeRuntime(dir / "n3", unknownReplicaCfg, 3, unknownOptions);
        unknownReplica->runtime->start();
        AKK_CLUSTER_CHECK(unknownReplica->runtime->role() == NodeRole::REPLICA);

        std::this_thread::sleep_for(std::chrono::milliseconds{300});
        const std::string key = "unknown-replica-key";
        const std::string value = "unknown-replica-value";
        bool rejected = false;
        try {
            primary->runtime->shipEntry(1, ReplOpType::PUT, bytesOf(key), bytesOf(value), 0, 1);
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        AKK_CLUSTER_CHECK(rejected);
        AKK_CLUSTER_CHECK(!unknownReplica->hasState(1, key, value));

        unknownReplica->runtime->close();
        primary->runtime->close();
    }

    void testClusterRuntimeRejectsDuplicateReplicaFromAckQuorum() {
        const auto dir = makeTempDir("cluster-runtime-duplicate-replica");
        ConsistencyOptions consistency;
        consistency.mode = ConsistencyMode::PRIMARY_ACK;
        consistency.writeConsistency = WriteConsistency::ALL_CONFIGURED;
        consistency.ackTimeoutAction = AckTimeoutAction::FAIL_WRITE;
        consistency.ackTimeoutMs = 100;

        const ClusterConfig cfg{
            {
                node(1, 20341, 20441),
                node(2, 20342, 20442),
                node(3, 20343, 20443),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            consistency,
        };

        ClusterRuntimeOptions primaryOptions;
        primaryOptions.startupRole = NodeStartupRole::PRIMARY;
        auto primary = makeRuntime(dir / "n1", cfg, 1, primaryOptions);
        primary->runtime->start();
        AKK_CLUSTER_CHECK(primary->runtime->role() == NodeRole::PRIMARY);

        ClusterRuntimeOptions replicaOptions;
        replicaOptions.startupRole = NodeStartupRole::REPLICA;
        replicaOptions.primaryNodeId = 1;
        auto firstReplica = makeRuntime(dir / "n2a", cfg, 2, replicaOptions);
        auto duplicateReplica = makeRuntime(dir / "n2b", cfg, 2, replicaOptions);
        firstReplica->runtime->start();
        duplicateReplica->runtime->start();
        AKK_CLUSTER_CHECK(firstReplica->runtime->role() == NodeRole::REPLICA);
        AKK_CLUSTER_CHECK(duplicateReplica->runtime->role() == NodeRole::REPLICA);

        const std::string key = "duplicate-replica-key";
        const std::string value = "duplicate-replica-value";
        bool rejected = false;
        try {
            primary->runtime->shipEntry(1, ReplOpType::PUT, bytesOf(key), bytesOf(value), 0, 1);
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        AKK_CLUSTER_CHECK(rejected);
        const bool firstApplied = firstReplica->hasState(1, key, value);
        const bool duplicateApplied = duplicateReplica->hasState(1, key, value);
        AKK_CLUSTER_CHECK(firstApplied != duplicateApplied);

        duplicateReplica->runtime->close();
        firstReplica->runtime->close();
        primary->runtime->close();
    }

    void testRaftMajorityFailureRejectsWrite() {
        const auto dir = makeTempDir("raft-majority-failure");
        const ClusterConfig cfg{
            {
                node(1, 20121, 20221),
                node(2, 20122, 20222),
                node(3, 20123, 20223),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 250},
        };

        auto n1 = makeRuntime(dir / "n1", cfg, 1);
        n1->runtime->start();

        AKK_CLUSTER_CHECK(!waitUntil([&] { return n1->runtime->role() == NodeRole::PRIMARY; }, std::chrono::milliseconds{1200}));

        const std::string key = "no-quorum-key";
        const std::string value = "no-quorum-value";
        bool rejected = false;
        try {
            n1->runtime->shipEntry(1, ReplOpType::PUT, bytesOf(key), bytesOf(value), 0, 1);
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        AKK_CLUSTER_CHECK(rejected);

        n1->runtime->close();
    }

    void testRaftLogTrailingBytesRecoveryPolicy() {
        const auto dir = makeTempDir("raft-log-trailing-recovery");
        const ClusterConfig cfg{
            {
                node(1, 21381, 21481),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE, .ackTimeoutMs = 300},
        };

        ClusterRuntimeOptions options;
        options.startupRole = NodeStartupRole::PRIMARY;
        {
            auto first = makeRuntime(dir / "n1", cfg, 1, options);
            first->runtime->start();
            RuntimeHarness* nodes[] = {first.get()};
            runOnLeader(
                nodes,
                [](RuntimeHarness& leader) {
                    leader.runtime->shipEntry(1, ReplOpType::PUT, bytesOf("raft-log-key"), bytesOf("raft-log-value"), 0, leader.nodeId);
                },
                std::chrono::milliseconds{8000}
            );
            first->runtime->close();
        }

        const auto logPath = dir / "n1" / "cluster-raft.log";
        const auto originalSize = std::filesystem::file_size(logPath);
        appendTextFile(logPath, "trailing-bytes");

        bool rejected = false;
        try {
            auto rejectedRuntime = makeRuntime(dir / "n1", cfg, 1, options);
            (void)rejectedRuntime;
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        AKK_CLUSTER_CHECK(rejected);
        AKK_CLUSTER_CHECK(std::filesystem::file_size(logPath) > originalSize);

        options.raftLogRecoveryAction = RaftLogRecoveryAction::TRUNCATE_UNCOMMITTED_TAIL;
        rejected = false;
        try { (void)makeRuntime(dir / "n1", cfg, 1, options); }
        catch (const std::runtime_error&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected); // Metadata corruption cannot be repaired by truncating data segments.
    }

    void testRaftRejectsBlobPayloadsByDefault() {
        const auto dir = makeTempDir("raft-blob-reject");
        const ClusterConfig cfg{
            {
                node(1, 21391, 21491),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE, .ackTimeoutMs = 300},
        };

        {
            auto options = akkaradb::engine::AkkEngineOptions{};
            options.paths.dataDir = dir / "engine";
            options.components.clusterEnabled = true;
            options.components.blobEnabled = true;
            options.cluster.config = cfg;
            options.cluster.runtime.startupRole = NodeStartupRole::PRIMARY;

            bool rejected = false;
            try {
                auto engine = akkaradb::engine::AkkEngine::open(options);
                (void)engine;
            }
            catch (const std::invalid_argument&) {
                rejected = true;
            }
            AKK_CLUSTER_CHECK(rejected);
        }

        ClusterRuntimeOptions options;
        options.startupRole = NodeStartupRole::PRIMARY;
        auto runtime = makeRuntime(dir / "runtime", cfg, 1, options);
        runtime->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return runtime->runtime->role() == NodeRole::PRIMARY; }));

        bool rejected = false;
        try {
            runtime->runtime->shipBlob(1, 1, bytesOf("raft-blob-payload"));
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        AKK_CLUSTER_CHECK(rejected);
        runtime->runtime->close();
    }

    void testRaftPrimarySideOnlyBlobPolicy() {
        const auto dir = makeTempDir("raft-blob-primary-side-only");
        const ClusterConfig cfg{
            {
                node(1, 21392, 21492),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE, .ackTimeoutMs = 300},
        };

        {
            auto options = akkaradb::engine::AkkEngineOptions{};
            options.paths.dataDir = dir / "engine";
            options.components.clusterEnabled = true;
            options.components.blobEnabled = true;
            options.cluster.config = cfg;
            options.cluster.runtime.startupRole = NodeStartupRole::PRIMARY;
            options.cluster.runtime.raftBlobPolicy = RaftBlobPolicy::PRIMARY_SIDE_ONLY;
            writeNodeIdFile(options.paths.dataDir / "node.id", 1);

            auto engine = akkaradb::engine::AkkEngine::open(options);
            AKK_CLUSTER_CHECK(engine != nullptr);
            engine->close();
        }

        ClusterRuntimeOptions leaderOptions;
        leaderOptions.startupRole = NodeStartupRole::PRIMARY;
        leaderOptions.raftBlobPolicy = RaftBlobPolicy::PRIMARY_SIDE_ONLY;
        auto leader = makeRuntime(dir / "leader-runtime", cfg, 1, leaderOptions);
        leader->runtime->start();
        RuntimeHarness* leaderNodes[] = {leader.get()};
        runOnLeader(
            leaderNodes,
            [](RuntimeHarness& currentLeader) {
                currentLeader.runtime->shipBlob(1, 1, bytesOf("raft-primary-side-blob-payload"));
            },
            std::chrono::milliseconds{8000}
        );
        leader->runtime->close();

        const ClusterConfig followerCfg{
            {
                node(1, 21393, 21493),
                node(2, 21394, 21494),
                node(3, 21395, 21495),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE, .ackTimeoutMs = 300},
        };
        ClusterRuntimeOptions followerOptions;
        followerOptions.raftBlobPolicy = RaftBlobPolicy::PRIMARY_SIDE_ONLY;
        auto follower = makeRuntime(dir / "follower-runtime", followerCfg, 2, followerOptions);

        bool rejected = false;
        try {
            follower->runtime->shipBlob(1, 1, bytesOf("raft-follower-blob-payload"));
        }
        catch (const std::runtime_error&) {
            rejected = true;
        }
        AKK_CLUSTER_CHECK(rejected);
        follower->runtime->close();
    }

    void testRaftLogBlobPolicyReplicatesPayload() {
        const auto dir = makeTempDir("raft-blob-log-policy");
        const ClusterConfig cfg{
            {
                node(1, 21401, 21501),
                node(2, 21402, 21502),
                node(3, 21403, 21503),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE, .ackTimeoutMs = 300},
        };

        ClusterRuntimeOptions options;
        options.raftBlobPolicy = RaftBlobPolicy::RAFT_LOG;
        options.raftBlobChunkSizeBytes = 5;
        auto n1 = makeRuntime(dir / "n1", cfg, 1, options);
        auto n2 = makeRuntime(dir / "n2", cfg, 2, options);
        auto n3 = makeRuntime(dir / "n3", cfg, 3, options);
        RuntimeHarness* nodes[] = {n1.get(), n2.get(), n3.get()};
        for (auto* n : nodes) { n->runtime->start(); }

        AKK_CLUSTER_CHECK(waitUntil([&] { return findLeader(nodes) != nullptr; }, std::chrono::milliseconds{8000}));
        const std::string payload = "raft-log-blob-payload";
        runOnLeader(nodes, [&](RuntimeHarness& leader) { leader.runtime->shipBlob(42, 420, bytesOf(payload)); });
        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                for (const auto* n : nodes) {
                    if (!n->hasBlob(42, 420, payload)) { return false; }
                }
                return true;
            },
            std::chrono::milliseconds{8000}
        ));

        for (auto* n : nodes) { n->runtime->close(); }

        const ClusterConfig singleNodeCfg{
            {
                node(1, 21404, 21504),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE, .ackTimeoutMs = 300},
        };
        auto first = makeRuntime(dir / "single", singleNodeCfg, 1, options);
        first->runtime->start();
        RuntimeHarness* singleNodes[] = {first.get()};
        runOnLeader(
            singleNodes,
            [](RuntimeHarness& leader) {
                leader.runtime->shipBlob(7, 70, bytesOf("persisted-raft-log-blob"));
            },
            std::chrono::milliseconds{8000}
        );
        first->runtime->close();
        first.reset();

        auto recovered = makeRuntime(dir / "single", singleNodeCfg, 1, options);
        recovered->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return recovered->runtime->role() == NodeRole::PRIMARY; }));
        AKK_CLUSTER_CHECK(recovered->hasBlob(7, 70, "persisted-raft-log-blob"));
        recovered->runtime->close();
    }

    void testRaftLogBlobPolicyExternalizesEngineValue() {
        const auto dir = makeTempDir("raft-blob-log-engine");
        const ClusterConfig cfg{
            {
                node(1, 21405, 21505),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE, .ackTimeoutMs = 300},
        };

        auto options = akkaradb::engine::AkkEngineOptions{};
        options.paths.dataDir = dir / "engine";
        options.components.clusterEnabled = true;
        options.components.blobEnabled = true;
        options.blob.thresholdBytes = 4;
        options.cluster.config = cfg;
        options.cluster.runtime.raftBlobPolicy = RaftBlobPolicy::RAFT_LOG;
        options.cluster.runtime.raftBlobChunkSizeBytes = 5;
        writeNodeIdFile(options.paths.dataDir / "node.id", 1);

        auto engine = akkaradb::engine::AkkEngine::open(options);
        akkaradb::engine::AkkEngine* nodes[] = {engine.get()};
        const std::string key = "raft-log-engine-blob-key";
        const std::string value = "raft-log-engine-blob-value";
        runOnEngineLeader(
            nodes,
            [&](akkaradb::engine::AkkEngine& leader) { leader.put(bytesOf(key), bytesOf(value)); },
            std::chrono::milliseconds{8000}
        );
        const auto stored = engine->get(bytesOf(key));
        AKK_CLUSTER_CHECK(stored.has_value());
        AKK_CLUSTER_CHECK(textOf(*stored) == value);
        AKK_CLUSTER_CHECK(engine->stats().blobPutsTotal >= 1);
        engine->close();
    }

    void testRaftEngineLeaderAppliesMutationOnce() {
        const auto dir = makeTempDir("raft-engine-apply-once");
        const ClusterConfig cfg{
            {
                node(1, 21409, 21509),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE, .ackTimeoutMs = 300},
        };

        auto options = akkaradb::engine::AkkEngineOptions{};
        options.paths.dataDir = dir / "engine";
        options.components.clusterEnabled = true;
        options.components.blobEnabled = false;
        options.components.versionLogEnabled = true;
        options.cluster.config = cfg;
        options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        writeNodeIdFile(options.paths.dataDir / "node.id", 1);

        auto engine = akkaradb::engine::AkkEngine::open(options);
        akkaradb::engine::AkkEngine* nodes[] = {engine.get()};
        const std::string key = "raft-apply-once-key";
        const std::string value = "raft-apply-once-value";
        runOnEngineLeader(
            nodes,
            [&](akkaradb::engine::AkkEngine& leader) { leader.put(bytesOf(key), bytesOf(value)); },
            std::chrono::milliseconds{8000}
        );
        const auto stored = engine->get(bytesOf(key));
        AKK_CLUSTER_CHECK(stored.has_value());
        AKK_CLUSTER_CHECK(textOf(*stored) == value);
        const auto history = engine->history(bytesOf(key));
        AKK_CLUSTER_CHECK(history.size() == 1);
        AKK_CLUSTER_CHECK(engine->stats().putsTotal == 1);
        engine->close();
    }

    void testRaftEngineLeadershipTransferApi() {
        const auto dir = makeTempDir("raft-engine-leader-transfer-api");
        const ClusterConfig cfg{
            {
                node(1, 21406, 21506),
                node(2, 21407, 21507),
                node(3, 21408, 21508),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE, .ackTimeoutMs = 500},
        };

        auto makeOptions = [&](uint64_t nodeId) {
            auto options = akkaradb::engine::AkkEngineOptions{};
            options.paths.dataDir = dir / ("n" + std::to_string(nodeId));
            options.components.clusterEnabled = true;
            options.components.blobEnabled = false;
            options.cluster.config = cfg;
            options.cluster.runtime.transportMode = TransportMode::PLAIN;
            writeNodeIdFile(options.paths.dataDir / "node.id", nodeId);
            return options;
        };

        auto n1 = akkaradb::engine::AkkEngine::open(makeOptions(1));
        auto n2 = akkaradb::engine::AkkEngine::open(makeOptions(2));
        auto n3 = akkaradb::engine::AkkEngine::open(makeOptions(3));
        akkaradb::engine::AkkEngine* nodes[] = {n1.get(), n2.get(), n3.get()};
        AKK_CLUSTER_CHECK(waitUntil([&] { return findEngineLeader(nodes) != nullptr; }, std::chrono::milliseconds{20000}));

        auto* leader = findEngineLeader(nodes);
        AKK_CLUSTER_CHECK(leader != nullptr);
        const uint64_t targetNodeId = leader == n1.get() ? 2 : 1;
        leader->transferClusterLeadership(targetNodeId);
        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                const auto* current = findEngineLeader(nodes);
                if (current == nullptr) { return false; }
                return current->stats().nodeId == targetNodeId;
            },
            std::chrono::milliseconds{15000}
        ));

        n3->close();
        n2->close();
        n1->close();
    }

    void testRaftEngineLargeBlobSnapshotCatchUp() {
        const auto dir = makeTempDir("raft-engine-blob-snapshot");
        const ClusterConfig cfg{
            {
                node(1, 21411, 21511),
                node(2, 21412, 21512),
                node(3, 21413, 21513),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE, .ackTimeoutMs = 2000},
        };

        auto makeOptions = [&](uint64_t nodeId) {
            auto options = akkaradb::engine::AkkEngineOptions{};
            options.paths.dataDir = dir / ("n" + std::to_string(nodeId));
            options.components.clusterEnabled = true;
            options.components.blobEnabled = true;
            options.blob.thresholdBytes = 4;
            options.cluster.config = cfg;
            options.cluster.runtime.transportMode = TransportMode::PLAIN;
            options.cluster.runtime.raftBlobPolicy = RaftBlobPolicy::RAFT_LOG;
            options.cluster.runtime.raftBlobChunkSizeBytes = 17;
            options.cluster.runtime.raftSnapshot.minLogEntries = 1;
            writeNodeIdFile(options.paths.dataDir / "node.id", nodeId);
            return options;
        };

        const auto staleExport = dir / "n1" / "cluster-snapshot-exports" / "interrupted.tmp";
        writeTextFile(staleExport, "incomplete-export");
        auto n1 = akkaradb::engine::AkkEngine::open(makeOptions(1));
        AKK_CLUSTER_CHECK(!std::filesystem::exists(staleExport));
        auto n3 = akkaradb::engine::AkkEngine::open(makeOptions(3));
        akkaradb::engine::AkkEngine* liveNodes[] = {n1.get(), n3.get()};
        AKK_CLUSTER_CHECK(waitUntil([&] { return findEngineLeader(liveNodes) != nullptr; }, std::chrono::milliseconds{20000}));

        const std::string key = "raft-engine-large-blob-snapshot-key";
        const std::vector<uint8_t> value = deterministicBytes(257, 0xA44A'5EED'B10B'0001ull);
        runOnEngineLeader(
            liveNodes,
            [&](akkaradb::engine::AkkEngine& leader) {
                leader.put(bytesOf(key), std::span<const uint8_t>{value.data(), value.size()});
            },
            std::chrono::milliseconds{20000}
        );

        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                for (auto* engine : liveNodes) {
                    const auto stored = engineGet(*engine, key);
                    if (!stored || *stored != value) { return false; }
                }
                return true;
            },
            std::chrono::milliseconds{10000}
        ));
        AKK_CLUSTER_CHECK(n1->stats().blob.blobsWritten > 0 || n3->stats().blob.blobsWritten > 0);
        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                const auto* leader = findEngineLeader(liveNodes);
                if (leader == nullptr) { return false; }
                const auto leaderDir = leader == n1.get() ? dir / "n1" : dir / "n3";
                return readRaftLogLastIncludedIndex(leaderDir / "cluster-raft.log") > 0;
            },
            std::chrono::milliseconds{5000}
        ));

        auto n2 = akkaradb::engine::AkkEngine::open(makeOptions(2));
        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                const auto stored = engineGet(*n2, key);
                return stored.has_value() && *stored == value;
            },
            std::chrono::milliseconds{15000}
        ));
        AKK_CLUSTER_CHECK(!std::filesystem::exists(dir / "n2" / "replication-snapshot.staging"));
        for (const auto& nodeDir : {dir / "n1", dir / "n3"}) {
            const auto exportDir = nodeDir / "cluster-snapshot-exports";
            AKK_CLUSTER_CHECK(waitUntil([&] {
                return !std::filesystem::exists(exportDir) || std::filesystem::is_empty(exportDir);
            }, std::chrono::milliseconds{5000}));
        }

        n2->close();
        n3->close();
        n1->close();
    }

    void testRaftEngineSnapshotStagingCrcRejectsCorruption() {
        const auto dir = makeTempDir("raft-engine-snapshot-staging-crc");
        const ClusterConfig cfg{
            {
                node(1, 21431, 21531),
                node(2, 21432, 21532),
                node(3, 21433, 21533),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE, .ackTimeoutMs = 2000},
        };

        auto makeOptions = [&](uint64_t nodeId) {
            auto options = akkaradb::engine::AkkEngineOptions{};
            options.paths.dataDir = dir / ("n" + std::to_string(nodeId));
            options.components.clusterEnabled = true;
            options.components.blobEnabled = true;
            options.blob.thresholdBytes = 4;
            options.cluster.config = cfg;
            options.cluster.runtime.transportMode = TransportMode::PLAIN;
            options.cluster.runtime.raftBlobPolicy = RaftBlobPolicy::RAFT_LOG;
            options.cluster.runtime.raftBlobChunkSizeBytes = 19;
            options.cluster.runtime.raftSnapshot.minLogEntries = 1;
            writeNodeIdFile(options.paths.dataDir / "node.id", nodeId);
            return options;
        };

        auto n1 = akkaradb::engine::AkkEngine::open(makeOptions(1));
        auto n3 = akkaradb::engine::AkkEngine::open(makeOptions(3));
        akkaradb::engine::AkkEngine* liveNodes[] = {n1.get(), n3.get()};
        AKK_CLUSTER_CHECK(waitUntil([&] { return findEngineLeader(liveNodes) != nullptr; }, std::chrono::milliseconds{20000}));

        const std::string key = "raft-engine-snapshot-staging-crc-key";
        const std::vector<uint8_t> value = deterministicBytes(233, 0xA44A'5EED'5A6E'C001ull);
        runOnEngineLeader(
            liveNodes,
            [&](akkaradb::engine::AkkEngine& leader) {
                leader.put(bytesOf(key), std::span<const uint8_t>{value.data(), value.size()});
            },
            std::chrono::milliseconds{20000}
        );
        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                for (auto* engine : liveNodes) {
                    const auto stored = engineGet(*engine, key);
                    if (!stored || *stored != value) { return false; }
                }
                return true;
            },
            std::chrono::milliseconds{10000}
        ));
        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                const auto* leader = findEngineLeader(liveNodes);
                if (leader == nullptr) { return false; }
                const auto leaderDir = leader == n1.get() ? dir / "n1" : dir / "n3";
                return readRaftLogLastIncludedIndex(leaderDir / "cluster-raft.log") > 0;
            },
            std::chrono::milliseconds{5000}
        ));

        ScopedSnapshotStagingCorruption corruption{"snapshot.finish.before_staging_apply"};
        auto n2 = akkaradb::engine::AkkEngine::open(makeOptions(2));
        const auto stagingPath = dir / "n2" / "replication-snapshot.staging";
        const auto corruptionMarkerPath = stagingPath.string() + ".corrupted";
        AKK_CLUSTER_CHECK(waitUntil(
            [&] { return std::filesystem::exists(corruptionMarkerPath); },
            std::chrono::milliseconds{10000}
        ));
        corruption.clear();
        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                const auto stored = engineGet(*n2, key);
                return stored.has_value() && *stored == value;
            },
            std::chrono::milliseconds{20000}
        ));
        AKK_CLUSTER_CHECK(!std::filesystem::exists(stagingPath));

        n2->close();
        n3->close();
        n1->close();
    }

    void testRaftEngineLargeBlobLeaderChurnKeepsBlobIdsUnique() {
        const auto dir = makeTempDir("raft-engine-blob-leader-churn");
        const ClusterConfig cfg{
            {
                node(1, 21421, 21521),
                node(2, 21422, 21522),
                node(3, 21423, 21523),
            },
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE, .ackTimeoutMs = 500},
        };

        auto makeOptions = [&](uint64_t nodeId) {
            auto options = akkaradb::engine::AkkEngineOptions{};
            options.paths.dataDir = dir / ("n" + std::to_string(nodeId));
            options.components.clusterEnabled = true;
            options.components.blobEnabled = true;
            options.blob.thresholdBytes = 4;
            options.cluster.config = cfg;
            options.cluster.runtime.transportMode = TransportMode::PLAIN;
            options.cluster.runtime.raftBlobPolicy = RaftBlobPolicy::RAFT_LOG;
            options.cluster.runtime.raftBlobChunkSizeBytes = 13;
            writeNodeIdFile(options.paths.dataDir / "node.id", nodeId);
            return options;
        };

        auto n1 = akkaradb::engine::AkkEngine::open(makeOptions(1));
        auto n2 = akkaradb::engine::AkkEngine::open(makeOptions(2));
        auto n3 = akkaradb::engine::AkkEngine::open(makeOptions(3));
        akkaradb::engine::AkkEngine* allNodes[] = {n1.get(), n2.get(), n3.get()};
        AKK_CLUSTER_CHECK(waitUntil([&] { return findEngineLeader(allNodes) != nullptr; }, std::chrono::milliseconds{20000}));

        const std::string key1 = "raft-engine-leader-churn-blob-key-1";
        const std::string key2 = "raft-engine-leader-churn-blob-key-2";
        const std::vector<uint8_t> value1 = deterministicBytes(129, 0xA44A'5EED'C001'0001ull);
        const std::vector<uint8_t> value2 = deterministicBytes(131, 0xA44A'5EED'C001'0002ull);

        runOnEngineLeader(
            allNodes,
            [&](akkaradb::engine::AkkEngine& leader) {
                leader.put(bytesOf(key1), std::span<const uint8_t>{value1.data(), value1.size()});
            },
            std::chrono::milliseconds{20000}
        );
        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                for (auto* engine : allNodes) {
                    const auto stored = engineGet(*engine, key1);
                    if (!stored || *stored != value1) { return false; }
                }
                return true;
            },
            std::chrono::milliseconds{15000}
        ));

        auto* oldLeader = findEngineLeader(allNodes);
        AKK_CLUSTER_CHECK(oldLeader != nullptr);
        oldLeader->close();

        std::vector<akkaradb::engine::AkkEngine*> remaining;
        for (auto* engine : allNodes) {
            if (engine != oldLeader) { remaining.push_back(engine); }
        }
        auto remainingSpan = [&]() {
            return std::span<akkaradb::engine::AkkEngine* const>{remaining.data(), remaining.size()};
        };
        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                auto* leader = findEngineLeader(remainingSpan());
                return leader != nullptr && leader != oldLeader;
            },
            std::chrono::milliseconds{20000}
        ));

        runOnEngineLeader(
            remainingSpan(),
            [&](akkaradb::engine::AkkEngine& leader) {
                leader.put(bytesOf(key2), std::span<const uint8_t>{value2.data(), value2.size()});
            },
            std::chrono::milliseconds{20000}
        );
        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                for (auto* engine : remaining) {
                    const auto stored1 = engineGet(*engine, key1);
                    const auto stored2 = engineGet(*engine, key2);
                    if (!stored1 || *stored1 != value1 || !stored2 || *stored2 != value2) { return false; }
                }
                return true;
            },
            std::chrono::milliseconds{15000}
        ));

        n3->close();
        n2->close();
        n1->close();
    }

    void runDeterministicClusterSoak(uint64_t seed, uint32_t rounds, const std::filesystem::path& executable) {
        if (rounds == 0 || rounds > 10'000) { throw std::invalid_argument("cluster soak rounds must be in [1, 10000]"); }
        const uint64_t originalSeed = seed;
        auto next = [&]() {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            return seed;
        };
        for (uint32_t round = 0; round < rounds; ++round) {
            std::array<unsigned, 8> order{0, 1, 2, 3, 4, 5, 6, 7};
            for (size_t i = order.size() - 1; i != 0; --i) { std::swap(order[i], order[next() % (i + 1)]); }
            for (const auto scenario : order) {
                const char* name = nullptr;
                switch (scenario) {
                    case 0: name = "frame-fault-latency-resume"; break;
                    case 1: name = "large-plain-transfer"; break;
                    case 2: name = "large-secure-transfer"; break;
                    case 3: name = "disconnect-reconnect"; break;
                    case 4: name = "raft-apply-crash"; break;
                    case 5: name = "raft-snapshot-catchup"; break;
                    case 6: name = "raft-leader-transfer"; break;
                    default: name = "raft-large-blob-leader-churn"; break;
                }
                std::fprintf(stderr, "[cluster-soak] seed=%llu round=%u scenario=%s\n",
                    static_cast<unsigned long long>(originalSeed), round, name);
                switch (scenario) {
                    case 0: testChunkedTransferValidation(); break;
                    case 1: testLargeReplicationTransfer(false); break;
                    case 2: testLargeReplicationTransfer(true); break;
                    case 3: testReplicationServerReapsDisconnectedReplicas(); break;
                    case 4: testRetryAfterApplyCrash(executable); break;
                    case 5: testRaftSnapshotInstallForCompactedFollower(); break;
                    case 6: testRaftLeaderTransfer(); break;
                    default: testRaftEngineLargeBlobLeaderChurnKeepsBlobIdsUnique(); break;
                }
            }
        }
    }
}

int main(int argc, char** argv) {
    try {
        akkaradb::test::installMsvcTestErrorHandlers();
        enableDeterministicRaftElection();
        AKK_CLUSTER_CHECK(akkaradb_cluster_register());
        if (argc == 4 && std::string_view{argv[1]} == "--stripe-crash-writer") { return runStripeCrashWriter(argv[2], argv[3]); }
        if (argc == 4 && std::string_view{argv[1]} == "--raft-log-crash-writer") { return runRaftLogCrashWriter(argv[2], argv[3]); }
        if (argc == 4 && std::string_view{argv[1]} == "--retry-crash-writer") { return runRetryCrashWriter(argv[2], argv[3]); }
        const auto executable = std::filesystem::absolute(argv[0]);
        if (argc == 2 && std::string_view{argv[1]} == "--replication-catchup") {
            // Focused coverage for bounded bootstrap, live cutover, and worker cleanup.
            testReplicationServerStreamsSnapshotCatchupThroughBoundedQueue();
            testReplicationServerRejectsOversizedCatchupFrame();
            testReplicationServerOrdersWritesCommittedDuringCatchup();
            testReplicationServerReapsDisconnectedReplicas();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--raft-snapshot-stream") {
            std::fprintf(stderr, "[cluster] testBlobReadPinDefersGcDeletion\n");
            testBlobReadPinDefersGcDeletion();
            std::fprintf(stderr, "[cluster] testRaftSnapshotCompactionThresholds\n");
            testRaftSnapshotCompactionThresholds();
            std::fprintf(stderr, "[cluster] testRaftSnapshotInstallForCompactedFollower\n");
            testRaftSnapshotInstallForCompactedFollower();
            std::fprintf(stderr, "[cluster] testRaftEngineLargeBlobSnapshotCatchUp\n");
            testRaftEngineLargeBlobSnapshotCatchUp();
            std::fprintf(stderr, "[cluster] testRaftEngineSnapshotStagingCrcRejectsCorruption\n");
            testRaftEngineSnapshotStagingCrcRejectsCorruption();
            return 0;
        }
        if (argc == 4 && std::string_view{argv[1]} == "--soak") {
            const auto seed = std::stoull(argv[2], nullptr, 0);
            const auto rounds = std::stoul(argv[3], nullptr, 0);
            runDeterministicClusterSoak(seed, rounds, executable);
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--stripe-concurrency") {
            testStripeFailureAndConcurrentReads();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--stripe-failover") {
            testStripeOwnerFailoverAndHandoff();
            return 0;
        }
        if (argc == 1 || (argc == 2 && std::string_view{argv[1]} == "--retry-transfer")) {
            std::fprintf(stderr, "[cluster] retry-safe requests\n");
            testRetrySafeEngineWrites();
            std::fprintf(stderr, "[cluster] Raft request policy handshake and observability\n");
            testRaftRejectsRequestPolicyMismatchAndReportsStats();
            std::fprintf(stderr, "[cluster] request journal delta compaction\n");
            testRequestJournalDeltaCompaction();
            std::fprintf(stderr, "[cluster] retry after apply crash\n");
            testRetryAfterApplyCrash(executable);
            std::fprintf(stderr, "[cluster] retry across snapshot, leadership and timeout\n");
            testRetryAcrossSnapshotAndLeaderChange();
            std::fprintf(stderr, "[cluster] chunked transfer validation\n");
            testChunkedTransferValidation();
            std::fprintf(stderr, "[cluster] large transfers PLAIN/SECURE\n");
            testLargeReplicationTransfer(false);
            testLargeReplicationTransfer(true);
            std::fprintf(stderr, "[cluster] read timeout preserves write ACK connection\n");
            testTimedOutReadKeepsWriteConnection();
            if (argc == 2) { return 0; }
        }
        if (argc == 1 || (argc == 2 && std::string_view{argv[1]} == "--cluster-hardening")) {
            std::fprintf(stderr, "[cluster] endpoint and pin validation\n");
            testClusterEndpointAndPinValidation();
            testSecureReplicationRequiresPeerPins();
            std::fprintf(stderr, "[cluster] parallel handshakes PLAIN/SECURE\n");
            testParallelReplicationHandshakes(false);
            testParallelReplicationHandshakes(true);
            std::fprintf(stderr, "[cluster] STRIPE read repair statistics\n");
            testStripeReadRepairStatistics();
            if (argc == 2) { return 0; }
        }
        if (argc == 2 && std::string_view{argv[1]} == "--raft") {
            testRaftRejectsRequestPolicyMismatchAndReportsStats();
            testRaftRejectsForeignClusterIdentity();
            testRaftRejectsBlobPolicyMismatch();
            testRequestJournalDeltaCompaction();
            testRaftPipelinedProposalsAndReadIndex();
            testRaftPipelinedProposalsAndReadIndex(true);
            testRaftConcurrentEngineWrites();
            std::fprintf(stderr, "[cluster] testRaftSegmentedLogRecovery\n");
            testRaftSegmentedLogRecovery(executable);
            std::fprintf(stderr, "[cluster] testRaftOptionsRoundtripAndValidation\n");
            testRaftOptionsRoundtripAndValidation();
            std::fprintf(stderr, "[cluster] testRaftElectionAndLoopbackReplication\n");
            testRaftElectionAndLoopbackReplication();
            std::fprintf(stderr, "[cluster] testRaftHardStateCorruptionFailsStartup\n");
            testRaftHardStateCorruptionFailsStartup();
            std::fprintf(stderr, "[cluster] testRaftRejectsForeignHardState\n");
            testRaftRejectsForeignHardState();
            std::fprintf(stderr, "[cluster] testRaftDoesNotReplayAppliedEntryAfterRestart\n");
            testRaftDoesNotReplayAppliedEntryAfterRestart();
            std::fprintf(stderr, "[cluster] testRaftSnapshotCompactionThresholds\n");
            testRaftSnapshotCompactionThresholds();
            std::fprintf(stderr, "[cluster] testRaftSnapshotInstallForCompactedFollower\n");
            testRaftSnapshotInstallForCompactedFollower();
            std::fprintf(stderr, "[cluster] testRaftLeaderTransfer\n");
            testRaftLeaderTransfer();
            std::fprintf(stderr, "[cluster] testRaftOnlineMembershipChange\n");
            testRaftOnlineMembershipChange();
            std::fprintf(stderr, "[cluster] testRaftMajorityFailureRejectsWrite\n");
            testRaftMajorityFailureRejectsWrite();
            std::fprintf(stderr, "[cluster] testRaftLogTrailingBytesRecoveryPolicy\n");
            testRaftLogTrailingBytesRecoveryPolicy();
            std::fprintf(stderr, "[cluster] testRaftRejectsBlobPayloadsByDefault\n");
            testRaftRejectsBlobPayloadsByDefault();
            std::fprintf(stderr, "[cluster] testRaftPrimarySideOnlyBlobPolicy\n");
            testRaftPrimarySideOnlyBlobPolicy();
            std::fprintf(stderr, "[cluster] testRaftLogBlobPolicyReplicatesPayload\n");
            testRaftLogBlobPolicyReplicatesPayload();
            std::fprintf(stderr, "[cluster] testRaftLogBlobPolicyExternalizesEngineValue\n");
            testRaftLogBlobPolicyExternalizesEngineValue();
            std::fprintf(stderr, "[cluster] testRaftEngineLeaderAppliesMutationOnce\n");
            testRaftEngineLeaderAppliesMutationOnce();
            std::fprintf(stderr, "[cluster] testRaftEngineLeadershipTransferApi\n");
            testRaftEngineLeadershipTransferApi();
            std::fprintf(stderr, "[cluster] testRaftEngineLargeBlobSnapshotCatchUp\n");
            testRaftEngineLargeBlobSnapshotCatchUp();
            std::fprintf(stderr, "[cluster] testRaftEngineSnapshotStagingCrcRejectsCorruption\n");
            testRaftEngineSnapshotStagingCrcRejectsCorruption();
            std::fprintf(stderr, "[cluster] testRaftEngineLargeBlobLeaderChurnKeepsBlobIdsUnique\n");
            testRaftEngineLargeBlobLeaderChurnKeepsBlobIdsUnique();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--raft-pipeline") {
            testRaftPipelinedProposalsAndReadIndex();
            testRaftPipelinedProposalsAndReadIndex(true);
            testRaftConcurrentEngineWrites();
            return 0;
        }
        std::fprintf(stderr, "[cluster] testStripePublicationCrashRecovery\n");
        testStripePublicationCrashRecovery(executable);
        std::fprintf(stderr, "[cluster] testStripeFailureAndConcurrentReads\n");
        testStripeFailureAndConcurrentReads();
        std::fprintf(stderr, "[cluster] testStripeOwnerFailoverAndHandoff\n");
        testStripeOwnerFailoverAndHandoff();
        std::fprintf(stderr, "[cluster] testRaftSegmentedLogRecovery\n");
        testRaftSegmentedLogRecovery(executable);
        if (argc == 2 && std::string_view{argv[1]} == "--storage") { return 0; }
        std::fprintf(stderr, "[cluster] testWalSnapshotTransactionRecovery\n");
        testWalSnapshotTransactionRecovery();
        std::fprintf(stderr, "[cluster] testRaftOptionsRoundtripAndValidation\n");
        testRaftOptionsRoundtripAndValidation();
        std::fprintf(stderr, "[cluster] testMirrorPrimaryRoundtripAndValidation\n");
        testMirrorPrimaryRoundtripAndValidation();
        std::fprintf(stderr, "[cluster] testReplFramingRejectsOversizedPayloadLength\n");
        testReplFramingRejectsOversizedPayloadLength();
        std::fprintf(stderr, "[cluster] testStripeControlFraming\n");
        testStripeControlFraming();
        std::fprintf(stderr, "[cluster] testReplHandshakeCarriesClusterGroupIdentity\n");
        testReplHandshakeCarriesClusterGroupIdentity();
        std::fprintf(stderr, "[cluster] testPartitionedAndStripeRouting\n");
        testPartitionedAndStripeRouting();
        std::fprintf(stderr, "[cluster] testStripeRuntimeStartsActivePlacement\n");
        testStripeRuntimeStartsActivePlacement();
        std::fprintf(stderr, "[cluster] testPartitionedRuntimeOwnerOnlyReplication\n");
        testPartitionedRuntimeOwnerOnlyReplication();
        std::fprintf(stderr, "[cluster] testPartitionedEngineOwnerLinearizableRead\n");
        testPartitionedEngineOwnerLinearizableRead();
        std::fprintf(stderr, "[cluster] testStripeEngineOwnerOnlyWritesAndCoordinatorReads\n");
        testStripeEngineOwnerOnlyWritesAndCoordinatorReads();
        std::fprintf(stderr, "[cluster] testStripeEngineRejectsDegradedWriteCommit\n");
        testStripeEngineRejectsDegradedWriteCommit();
        std::fprintf(stderr, "[cluster] testStripeEngineRejectsOnlineReconfiguration\n");
        testStripeEngineRejectsOnlineReconfiguration();
        std::fprintf(stderr, "[cluster] testClusterManagerRecoversPrimaryLeaseFromManifest\n");
        testClusterManagerRecoversPrimaryLeaseFromManifest();
        std::fprintf(stderr, "[cluster] testClusterManagerIgnoresExpiredManifestPrimaryLease\n");
        testClusterManagerIgnoresExpiredManifestPrimaryLease();
        std::fprintf(stderr, "[cluster] testClusterManagerRejectsAutoRoleAfterExpiredManifestLease\n");
        testClusterManagerRejectsAutoRoleAfterExpiredManifestLease();
        std::fprintf(stderr, "[cluster] testClusterManagerRejectsPrimaryStartupWithForeignLease\n");
        testClusterManagerRejectsPrimaryStartupWithForeignLease();
        std::fprintf(stderr, "[cluster] testClusterManagerDoesNotAutoPromoteRaftAfterExpiredManifestLease\n");
        testClusterManagerDoesNotAutoPromoteRaftAfterExpiredManifestLease();
        std::fprintf(stderr, "[cluster] testClusterManagerReportsManifestActiveNodes\n");
        testClusterManagerReportsManifestActiveNodes();
        std::fprintf(stderr, "[cluster] testClusterManagerContainsLeaseRenewalFailure\n");
        testClusterManagerContainsLeaseRenewalFailure();
        std::fprintf(stderr, "[cluster] testClusterRuntimeReportsLeaseRenewalFailure\n");
        testClusterRuntimeReportsLeaseRenewalFailure();
        std::fprintf(stderr, "[cluster] testStripeErasureCodecRecovery\n");
        testStripeErasureCodecRecovery();
        std::fprintf(stderr, "[cluster] testRsErasureCodecProperties\n");
        testRsErasureCodecProperties();
        std::fprintf(stderr, "[cluster] testErsCodecRecovery\n");
        testErsCodecRecovery();
        std::fprintf(stderr, "[cluster] testRaftElectionAndLoopbackReplication\n");
        testRaftElectionAndLoopbackReplication();
        std::fprintf(stderr, "[cluster] testRaftRejectsForeignClusterIdentity\n");
        testRaftRejectsForeignClusterIdentity();
        std::fprintf(stderr, "[cluster] testRaftRejectsBlobPolicyMismatch\n");
        testRaftRejectsBlobPolicyMismatch();
        testRaftPipelinedProposalsAndReadIndex();
        testRaftPipelinedProposalsAndReadIndex(true);
        testRaftConcurrentEngineWrites();
        std::fprintf(stderr, "[cluster] testRaftHardStateCorruptionFailsStartup\n");
        testRaftHardStateCorruptionFailsStartup();
        std::fprintf(stderr, "[cluster] testRaftRejectsForeignHardState\n");
        testRaftRejectsForeignHardState();
        std::fprintf(stderr, "[cluster] testRaftDoesNotReplayAppliedEntryAfterRestart\n");
        testRaftDoesNotReplayAppliedEntryAfterRestart();
        std::fprintf(stderr, "[cluster] testRaftSnapshotCompactionThresholds\n");
        testRaftSnapshotCompactionThresholds();
        std::fprintf(stderr, "[cluster] testRaftSnapshotInstallForCompactedFollower\n");
        testRaftSnapshotInstallForCompactedFollower();
        std::fprintf(stderr, "[cluster] testRaftLeaderTransfer\n");
        testRaftLeaderTransfer();
        std::fprintf(stderr, "[cluster] testRaftOnlineMembershipChange\n");
        testRaftOnlineMembershipChange();
        std::fprintf(stderr, "[cluster] testPrimaryAckPathStillUsesLegacyRuntime\n");
        testPrimaryAckPathStillUsesLegacyRuntime();
        std::fprintf(stderr, "[cluster] testReplicationServerStreamsSnapshotCatchupThroughBoundedQueue\n");
        testReplicationServerStreamsSnapshotCatchupThroughBoundedQueue();
        std::fprintf(stderr, "[cluster] testReplicationServerRejectsOversizedCatchupFrame\n");
        testReplicationServerRejectsOversizedCatchupFrame();
        std::fprintf(stderr, "[cluster] testReplicationServerOrdersWritesCommittedDuringCatchup\n");
        testReplicationServerOrdersWritesCommittedDuringCatchup();
        std::fprintf(stderr, "[cluster] testReplicationServerReapsDisconnectedReplicas\n");
        testReplicationServerReapsDisconnectedReplicas();
        std::fprintf(stderr, "[cluster] testClusterRuntimeDoesNotSerializePeerReadWithStats\n");
        testClusterRuntimeDoesNotSerializePeerReadWithStats();
        std::fprintf(stderr, "[cluster] testClusterRuntimeStartCanRetryAfterFailure\n");
        testClusterRuntimeStartCanRetryAfterFailure();
        std::fprintf(stderr, "[cluster] testClusterRuntimeReportsEndpointStartFailure\n");
        testClusterRuntimeReportsEndpointStartFailure();
        std::fprintf(stderr, "[cluster] testClusterRuntimeReportsPeerReadTimeout\n");
        testClusterRuntimeReportsPeerReadTimeout();
        std::fprintf(stderr, "[cluster] testAkkEngineRejectsUnsafeNativeClusterConfigurations\n");
        testAkkEngineRejectsUnsafeNativeClusterConfigurations();
        std::fprintf(stderr, "[cluster] testPrimaryAckFailWriteCommitsLocalBeforeAckFailure\n");
        testPrimaryAckFailWriteCommitsLocalBeforeAckFailure();
        std::fprintf(stderr, "[cluster] testSecureReplicationRequiresPeerPins\n");
        testSecureReplicationRequiresPeerPins();
        std::fprintf(stderr, "[cluster] testPrimaryAckAllTargetsFailsWithoutReplicas\n");
        testPrimaryAckAllTargetsFailsWithoutReplicas();
        std::fprintf(stderr, "[cluster] testReplicaRejectsImplicitNonRaftGroupSwitch\n");
        testReplicaRejectsImplicitNonRaftGroupSwitch();
        std::fprintf(stderr, "[cluster] testNonRaftMirrorRejectsSecondPrimary\n");
        testNonRaftMirrorRejectsSecondPrimary();
        std::fprintf(stderr, "[cluster] testNonRaftPrimaryRejectsCorruptGroupState\n");
        testNonRaftPrimaryRejectsCorruptGroupState();
        std::fprintf(stderr, "[cluster] testNonRaftPrimaryBacksUpCorruptGroupState\n");
        testNonRaftPrimaryBacksUpCorruptGroupState();
        std::fprintf(stderr, "[cluster] testNonRaftPrimaryReusesGroupStateAfterRestart\n");
        testNonRaftPrimaryReusesGroupStateAfterRestart();
        std::fprintf(stderr, "[cluster] testClusterRuntimeRejectsUnknownReplicaFromAckQuorum\n");
        testClusterRuntimeRejectsUnknownReplicaFromAckQuorum();
        std::fprintf(stderr, "[cluster] testClusterRuntimeRejectsDuplicateReplicaFromAckQuorum\n");
        testClusterRuntimeRejectsDuplicateReplicaFromAckQuorum();
        std::fprintf(stderr, "[cluster] testRaftMajorityFailureRejectsWrite\n");
        testRaftMajorityFailureRejectsWrite();
        std::fprintf(stderr, "[cluster] testRaftLogTrailingBytesRecoveryPolicy\n");
        testRaftLogTrailingBytesRecoveryPolicy();
        std::fprintf(stderr, "[cluster] testRaftRejectsBlobPayloadsByDefault\n");
        testRaftRejectsBlobPayloadsByDefault();
        std::fprintf(stderr, "[cluster] testRaftPrimarySideOnlyBlobPolicy\n");
        testRaftPrimarySideOnlyBlobPolicy();
        std::fprintf(stderr, "[cluster] testRaftLogBlobPolicyReplicatesPayload\n");
        testRaftLogBlobPolicyReplicatesPayload();
        std::fprintf(stderr, "[cluster] testRaftLogBlobPolicyExternalizesEngineValue\n");
        testRaftLogBlobPolicyExternalizesEngineValue();
        std::fprintf(stderr, "[cluster] testRaftEngineLeaderAppliesMutationOnce\n");
        testRaftEngineLeaderAppliesMutationOnce();
        std::fprintf(stderr, "[cluster] testRaftEngineLeadershipTransferApi\n");
        testRaftEngineLeadershipTransferApi();
        std::fprintf(stderr, "[cluster] testRaftEngineLargeBlobSnapshotCatchUp\n");
        testRaftEngineLargeBlobSnapshotCatchUp();
        std::fprintf(stderr, "[cluster] testRaftEngineSnapshotStagingCrcRejectsCorruption\n");
        testRaftEngineSnapshotStagingCrcRejectsCorruption();
        std::fprintf(stderr, "[cluster] testRaftEngineLargeBlobLeaderChurnKeepsBlobIdsUnique\n");
        testRaftEngineLargeBlobLeaderChurnKeepsBlobIdsUnique();
        return 0;
    }
    catch (const std::exception& ex) {
        std::fprintf(stderr, "cluster smoke failed: %s\n", ex.what());
        return 1;
    }
}

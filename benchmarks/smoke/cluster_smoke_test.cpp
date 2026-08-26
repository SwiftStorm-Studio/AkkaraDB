/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// benchmarks/smoke/cluster_smoke_test.cpp
#include "TestErrorHandlers.hpp"

#include "akk/core/record/MemHdr16.hpp"
#include "akk/engine/AkkEngine.hpp"
#include "akk/engine/cluster/ClusterConfig.hpp"
#include "akk/engine/cluster/ClusterManager.hpp"
#include "akk/engine/cluster/ClusterRuntime.hpp"
#include "akk/engine/cluster/ClusterRouter.hpp"
#include "akk/engine/cluster/ReplFraming.hpp"
#include "akk/engine/cluster/ReplicationClient.hpp"
#include "akk/engine/cluster/ReplicationServer.hpp"
#include "akk/engine/erasure/ErasureCodec.hpp"
#include "akk/engine/erasure/ErasureCodecExt.hpp"
#include "akk/engine/manifest/Manifest.hpp"
#include "akk/engine/memtable/MemTable.hpp"
#include "akk/engine/wal/WalFraming.hpp"
#include "akk/engine/wal/WalRecovery.hpp"
#include "akk/engine/wal/WalWriter.hpp"
#include "akk/cpu/CRC32C.hpp"

#include <array>
#include <atomic>
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
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
        ClusterRuntimeOptions options = {}
    ) {
        auto harness = std::make_unique<RuntimeHarness>();
        harness->nodeId = nodeId;
        harness->durableStatePath = dir / "harness-state.bin";
        harness->loadDurableState();
        options.transportMode = TransportMode::PLAIN;
        ClusterEngineCallbacks callbacks;
        callbacks.getLastSeq = [harness = harness.get()] { return harness->lastSeq.load(); };
        callbacks.getCurrentSeq = [harness = harness.get()] { return harness->lastSeq.load(); };
        callbacks.exportSnapshot = [harness = harness.get()]() -> std::optional<ClusterSnapshot> {
            std::lock_guard lock{harness->stateMutex};
            const uint64_t seq = harness->lastSeq.load();
            if (seq == 0) { return std::nullopt; }
            ClusterSnapshot snapshot;
            snapshot.seq = seq;
            snapshot.entries.push_back(ClusterHistoryEntry{
                .seq = seq,
                .sourceNodeId = harness->nodeId,
                .op = static_cast<uint8_t>(ReplOpType::PUT),
                .recordFlags = 0,
                .key = std::vector<uint8_t>{harness->lastKey.begin(), harness->lastKey.end()},
                .value = std::vector<uint8_t>{harness->lastValue.begin(), harness->lastValue.end()},
            });
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
        AKK_CLUSTER_CHECK(loaded.consistency().ackTimeoutAction == AckTimeoutAction::FAIL_WRITE);
        AKK_CLUSTER_CHECK(loaded.raft().membership.mode == RaftMembershipMode::JOINT_CONSENSUS);
        AKK_CLUSTER_CHECK(loaded.raft().membership.allowOnlineVoterChanges);

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
    }

    void testReplFramingRejectsOversizedPayloadLength() {
        std::vector<uint8_t> wire(ReplFrameHeader::SIZE);
        writeU32Le(wire, 0, ReplFrameHeader::MAGIC);
        wire[4] = static_cast<uint8_t>(ReplMsgType::ACK);
        writeU32Le(wire, 6, ReplFrameHeader::MAX_PAYLOAD_SIZE + 1);

        DecodedFrame decoded;
        AKK_CLUSTER_CHECK(!decodeFrame(wire, decoded));
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
        const ClusterConfig stripe{
            {
                node(1, 20311, 20411),
                node(2, 20312, 20412),
                node(3, 20313, 20413),
                node(4, 20314, 20414),
                node(5, 20315, 20415),
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

    void testClusterManagerAutoPromotesAfterExpiredManifestLease() {
        const auto dir = makeTempDir("manifest-expired-primary-lease-promote");
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

        auto primary = ClusterManager::create(dir, cfg, 1);
        primary->start();
        AKK_CLUSTER_CHECK(primary->role() == NodeRole::PRIMARY);
        AKK_CLUSTER_CHECK(primary->primaryNodeId() == 1);
        AKK_CLUSTER_CHECK(primary->primaryHost() == "127.0.0.1");
        AKK_CLUSTER_CHECK(primary->primaryReplPort() == 20441);
        primary->close();
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
                currentLeader.runtime->shipEntry(1, ReplOpType::PUT, bytesOf(key), bytesOf(value), 7, currentLeader.nodeId);
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
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 250},
        };

        ClusterRuntimeOptions options;
        options.raftBlobChunkSizeBytes = 5;
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
                return n2->snapshotsInstalled.load() == 1 && n2->hasState(2, key2, value2);
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
        };

        auto n1 = makeRuntime(dir / "n1", initialCfg, 1);
        auto n2 = makeRuntime(dir / "n2", initialCfg, 2);
        auto n3 = makeRuntime(dir / "n3", initialCfg, 3);
        auto n4 = makeRuntime(dir / "n4", joiningCfg, 4);

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

        n4->runtime->close();
        n3->runtime->close();
        n2->runtime->close();
        n1->runtime->close();

        AKK_CLUSTER_CHECK(readRaftLogLastIncludedIndex(dir / "n1" / "cluster-raft.log") > 0 ||
                          readRaftLogLastIncludedIndex(dir / "n2" / "cluster-raft.log") > 0 ||
                          readRaftLogLastIncludedIndex(dir / "n3" / "cluster-raft.log") > 0);

        n1 = makeRuntime(dir / "n1", initialCfg, 1);
        n2 = makeRuntime(dir / "n2", initialCfg, 2);
        n3 = makeRuntime(dir / "n3", initialCfg, 3);
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
        auto server = ReplicationServer::create(port, 1, [] { return uint64_t{0}; }, ack, {}, 0, {}, serverOptions);
        auto client = ReplicationClient::create("127.0.0.1", port, 2, [] { return uint64_t{0}; }, ack, clientOptions);

        server->start();
        client->start();
        std::this_thread::sleep_for(std::chrono::milliseconds{500});
        AKK_CLUSTER_CHECK(server->replicaCount() == 0);

        client->close();
        server->close();
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

    void testNonRaftPrimaryCreatesGroupInstanceIdentity() {
        const auto dir = makeTempDir("non-raft-group-instance");
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
        n3->runtime->start();
        AKK_CLUSTER_CHECK(n3->runtime->role() == NodeRole::PRIMARY);

        const auto group1 = readGroupState(dir / "n1" / "cluster.membership");
        const auto group3 = readGroupState(dir / "n3" / "cluster.membership");
        AKK_CLUSTER_CHECK(group1.groupId != 0);
        AKK_CLUSTER_CHECK(group3.groupId != 0);
        AKK_CLUSTER_CHECK(group1.groupId != group3.groupId);
        AKK_CLUSTER_CHECK(group1.primaryNodeId == 1);
        AKK_CLUSTER_CHECK(group3.primaryNodeId == 3);
        AKK_CLUSTER_CHECK(group1.groupEpoch == 1);
        AKK_CLUSTER_CHECK(group3.groupEpoch == 1);

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
        auto recovered = makeRuntime(dir / "n1", cfg, 1, options);
        AKK_CLUSTER_CHECK(std::filesystem::file_size(logPath) == originalSize);
        recovered->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return recovered->runtime->role() == NodeRole::PRIMARY; }));
        recovered->runtime->close();
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
            options.cluster.runtime.raftBlobChunkSizeBytes = 17;
            writeNodeIdFile(options.paths.dataDir / "node.id", nodeId);
            return options;
        };

        auto n1 = akkaradb::engine::AkkEngine::open(makeOptions(1));
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
            options.cluster.runtime.raftBlobChunkSizeBytes = 19;
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
}

int main() {
    try {
        akkaradb::test::installMsvcTestErrorHandlers();
        enableDeterministicRaftElection();
        AKK_CLUSTER_CHECK(akkaradb_cluster_register());
        testWalSnapshotTransactionRecovery();
        testRaftOptionsRoundtripAndValidation();
        testReplFramingRejectsOversizedPayloadLength();
        testReplHandshakeCarriesClusterGroupIdentity();
        testPartitionedAndStripeRouting();
        testClusterManagerRecoversPrimaryLeaseFromManifest();
        testClusterManagerIgnoresExpiredManifestPrimaryLease();
        testClusterManagerAutoPromotesAfterExpiredManifestLease();
        testClusterManagerDoesNotAutoPromoteRaftAfterExpiredManifestLease();
        testClusterManagerReportsManifestActiveNodes();
        testStripeErasureCodecRecovery();
        testRsErasureCodecProperties();
        testErsCodecRecovery();
        testRaftElectionAndLoopbackReplication();
        testRaftHardStateCorruptionFailsStartup();
        testRaftDoesNotReplayAppliedEntryAfterRestart();
        testRaftSnapshotInstallForCompactedFollower();
        testRaftLeaderTransfer();
        testRaftOnlineMembershipChange();
        testPrimaryAckPathStillUsesLegacyRuntime();
        testSecureReplicationRequiresPeerPins();
        testPrimaryAckAllTargetsFailsWithoutReplicas();
        testReplicaRejectsImplicitNonRaftGroupSwitch();
        testNonRaftPrimaryCreatesGroupInstanceIdentity();
        testNonRaftPrimaryRejectsCorruptGroupState();
        testNonRaftPrimaryBacksUpCorruptGroupState();
        testNonRaftPrimaryReusesGroupStateAfterRestart();
        testClusterRuntimeRejectsUnknownReplicaFromAckQuorum();
        testClusterRuntimeRejectsDuplicateReplicaFromAckQuorum();
        testRaftMajorityFailureRejectsWrite();
        testRaftLogTrailingBytesRecoveryPolicy();
        testRaftRejectsBlobPayloadsByDefault();
        testRaftPrimarySideOnlyBlobPolicy();
        testRaftLogBlobPolicyReplicatesPayload();
        testRaftLogBlobPolicyExternalizesEngineValue();
        testRaftEngineLargeBlobSnapshotCatchUp();
        testRaftEngineSnapshotStagingCrcRejectsCorruption();
        testRaftEngineLargeBlobLeaderChurnKeepsBlobIdsUnique();
        return 0;
    }
    catch (const std::exception& ex) {
        std::fprintf(stderr, "cluster smoke failed: %s\n", ex.what());
        return 1;
    }
}

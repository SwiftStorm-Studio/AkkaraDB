/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// benchmarks/smoke/cluster_smoke_test.cpp
#include "akk/engine/detail/ProtocolBulkWriter.hpp"
#ifdef _WIN32
#include "detail/CollectHistory.hpp"
#include <akk/engine/cluster/detail/ReconfigurationDeadline.hpp>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#else
#include <arpa/inet.h>
#include <sys/select.h>
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
#include "akk/engine/cluster/detail/RaftLogIndex.hpp"
#include "akk/engine/cluster/detail/BoundedExecutor.hpp"
#include "akk/engine/cluster/ClusterRouter.hpp"
#include "akk/engine/cluster/ReplFraming.hpp"
#include "akk/engine/cluster/ReplicationClient.hpp"
#include "akk/engine/cluster/ReplicationServer.hpp"
#include "akk/engine/cluster/detail/ReplicationTransfer.hpp"
#include "akk/engine/cluster/detail/ForwardDeadline.hpp"
#include "akk/engine/erasure/ErasureCodec.hpp"
#include "akk/engine/erasure/ErasureCodecExt.hpp"
#include "akk/engine/manifest/Manifest.hpp"
#include "akk/engine/memtable/MemTable.hpp"
#include "akk/engine/memtable/SkipListMemTable.hpp"
#include "akk/engine/wal/WalFraming.hpp"
#include "akk/engine/wal/WalRecovery.hpp"
#include "akk/engine/wal/WalWriter.hpp"
#include "akk/cpu/CRC32C.hpp"
#include "akk/crypto/Identity.hpp"
#include "akk/crypto/Random.hpp"

#include <array>
#include <atomic>
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <limits>
#include <map>
#include <set>
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

    [[nodiscard]] uint32_t snapshotValueCrc(std::span<const uint8_t> value) noexcept {
        return akkaradb::cpu::CRC32C(reinterpret_cast<const std::byte*>(value.data()), value.size());
    }

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
            .stripeMetadataPort = static_cast<uint16_t>(replPort + 1000),
            .capabilities = capabilities,
        };
    }

    ClusterId testClusterId(uint64_t tag) {
        ClusterId id{};
        for (size_t index = 0; index < id.size(); ++index) {
            id[index] = static_cast<uint8_t>((tag >> ((index % 8) * 8)) ^ (0x5DU + index * 17U));
        }
        return id;
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
        if (bytes.size() != 33) { throw std::runtime_error("invalid group state size at " + path.string() + ": " + std::to_string(bytes.size())); }
        AKK_CLUSTER_CHECK((std::string_view{reinterpret_cast<const char*>(bytes.data()), 5} == "AKCG1"));
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

    void setLeaseRenewalFailureTestEnvironment(bool enabled, bool failRenewal) {
#ifdef _WIN32
        AKK_CLUSTER_CHECK(_putenv_s("AKKARADB_TEST_LEASE_RENEW_INTERVAL_MS", enabled ? "10" : "") == 0);
        AKK_CLUSTER_CHECK(_putenv_s("AKKARADB_TEST_FAIL_LEASE_RENEWAL", enabled && failRenewal ? "1" : "") == 0);
#else
        if (enabled) {
            AKK_CLUSTER_CHECK(::setenv("AKKARADB_TEST_LEASE_RENEW_INTERVAL_MS", "10", 1) == 0);
            if (failRenewal) { AKK_CLUSTER_CHECK(::setenv("AKKARADB_TEST_FAIL_LEASE_RENEWAL", "1", 1) == 0); }
            else { AKK_CLUSTER_CHECK(::unsetenv("AKKARADB_TEST_FAIL_LEASE_RENEWAL") == 0); }
        }
        else {
            AKK_CLUSTER_CHECK(::unsetenv("AKKARADB_TEST_LEASE_RENEW_INTERVAL_MS") == 0);
            AKK_CLUSTER_CHECK(::unsetenv("AKKARADB_TEST_FAIL_LEASE_RENEWAL") == 0);
        }
#endif
    }

    class ScopedLeaseRenewalFailure {
        public:
            ScopedLeaseRenewalFailure() { setLeaseRenewalFailureTestEnvironment(true, false); }
            void trigger() { setLeaseRenewalFailureTestEnvironment(true, true); }
            ~ScopedLeaseRenewalFailure() { setLeaseRenewalFailureTestEnvironment(false, false); }
            ScopedLeaseRenewalFailure(const ScopedLeaseRenewalFailure&) = delete;
            ScopedLeaseRenewalFailure& operator=(const ScopedLeaseRenewalFailure&) = delete;
    };

    class ScopedPrimaryStepDown {
        public:
            ScopedPrimaryStepDown() {
#ifdef _WIN32
                AKK_CLUSTER_CHECK(_putenv_s("AKKARADB_TEST_LEASE_RENEW_INTERVAL_MS", "10") == 0);
#else
                AKK_CLUSTER_CHECK(::setenv("AKKARADB_TEST_LEASE_RENEW_INTERVAL_MS", "10", 1) == 0);
#endif
            }

            void trigger() {
#ifdef _WIN32
                AKK_CLUSTER_CHECK(_putenv_s("AKKARADB_TEST_STEP_DOWN_PRIMARY", "1") == 0);
#else
                AKK_CLUSTER_CHECK(::setenv("AKKARADB_TEST_STEP_DOWN_PRIMARY", "1", 1) == 0);
#endif
            }

            ~ScopedPrimaryStepDown() {
#ifdef _WIN32
                AKK_CLUSTER_CHECK(_putenv_s("AKKARADB_TEST_LEASE_RENEW_INTERVAL_MS", "") == 0);
                AKK_CLUSTER_CHECK(_putenv_s("AKKARADB_TEST_STEP_DOWN_PRIMARY", "") == 0);
#else
                AKK_CLUSTER_CHECK(::unsetenv("AKKARADB_TEST_LEASE_RENEW_INTERVAL_MS") == 0);
                AKK_CLUSTER_CHECK(::unsetenv("AKKARADB_TEST_STEP_DOWN_PRIMARY") == 0);
#endif
            }

            ScopedPrimaryStepDown(const ScopedPrimaryStepDown&) = delete;
            ScopedPrimaryStepDown& operator=(const ScopedPrimaryStepDown&) = delete;
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

    uint64_t readRaftLogHeaderValue(const std::filesystem::path& path, size_t offset) {
        std::array<uint8_t, 49> bytes{};
#ifdef _WIN32
        // Observing live metadata must not block its atomic replacement.
        const auto file = CreateFileW(path.c_str(), GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
        if (file == INVALID_HANDLE_VALUE) { return 0; }
        DWORD count = 0;
        const bool read = ReadFile(file, bytes.data(), static_cast<DWORD>(bytes.size()), &count, nullptr) != FALSE;
        CloseHandle(file);
        if (!read || count != bytes.size()) { return 0; }
#else
        std::ifstream in(path, std::ios::binary);
        if (!in.read(reinterpret_cast<char*>(bytes.data()), bytes.size())) { return 0; }
#endif
        const std::string_view magic{reinterpret_cast<const char*>(bytes.data()), 5};
        if (magic == "AKRL1" && offset + 8 <= bytes.size()) { return readU64Le(bytes, offset); }
        return 0;
    }

    uint64_t readRaftLogLastIncludedIndex(const std::filesystem::path& path) {
        return readRaftLogHeaderValue(path, 21);
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
        std::unordered_map<std::string, std::vector<uint8_t>> stripeMetadata;
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
        bool secure = false,
        std::function<ForwardResponse(const ForwardRequest&)> forward = {},
        std::function<void(ClusterEngineCallbacks&)> configure = {}
    ) {
        auto harness = std::make_unique<RuntimeHarness>();
        harness->nodeId = nodeId;
        harness->durableStatePath = dir / "harness-state.bin";
        harness->loadDurableState();
        options.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
        ClusterEngineCallbacks callbacks;
        callbacks.forward = std::move(forward);
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
        callbacks.commitStripeMetadata = [harness = harness.get()](
            uint64_t, std::span<const uint8_t> key, std::span<const uint8_t> metadata
        ) {
            std::lock_guard lock{harness->stateMutex};
            harness->stripeMetadata[std::string{reinterpret_cast<const char*>(key.data()), key.size()}] =
                std::vector<uint8_t>{metadata.begin(), metadata.end()};
        };
        callbacks.readStripeMetadata = [harness = harness.get()](std::span<const uint8_t> key)
            -> std::optional<std::vector<uint8_t>> {
            std::lock_guard lock{harness->stateMutex};
            const auto found = harness->stripeMetadata.find(
                std::string{reinterpret_cast<const char*>(key.data()), key.size()}
            );
            if (found == harness->stripeMetadata.end()) { return std::nullopt; }
            return found->second;
        };
        callbacks.exportSnapshot = [harness = harness.get()]() -> std::optional<ClusterSnapshot> {
            harness->snapshotsExported.fetch_add(1);
            std::lock_guard lock{harness->stateMutex};
            const uint64_t seq = harness->lastSeq.load();
            if (seq == 0) { return std::nullopt; }
            ClusterSnapshot snapshot;
            snapshot.seq = seq;
            const auto entry = std::make_shared<ClusterHistoryEntry>(ClusterHistoryEntry{
                    .seq = seq,
                    .sourceNodeId = harness->nodeId,
                    .op = static_cast<uint8_t>(ReplOpType::PUT),
                    .recordFlags = 0,
                    .key = std::vector<uint8_t>{harness->lastKey.begin(), harness->lastKey.end()},
                    .value = std::vector<uint8_t>{harness->lastValue.begin(), harness->lastValue.end()},
            });
            snapshot.forEachEntry = [entry](const ClusterSnapshot::EntryVisitor& visitor) {
                return visitor.beginEntry(entry->key, entry->value.size(), snapshotValueCrc(entry->value)) &&
                    visitor.appendValueChunk(0, entry->value) && visitor.finishEntry();
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
        callbacks.finishSnapshot = [harness = harness.get()](uint64_t snapshotSeq, uint64_t entryCount) {
            AKK_CLUSTER_CHECK(entryCount == 1);
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
        , uint64_t) {
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
        if (configure) { configure(callbacks); }
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
        if (const auto* routing = dynamic_cast<const ClusterRoutingError*>(&error)) {
            return routing->code == ClusterRoutingCode::NOT_OWNER || routing->code == ClusterRoutingCode::NO_TARGET ||
                routing->code == ClusterRoutingCode::FORWARD_UNAVAILABLE;
        }
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

    std::optional<std::vector<uint8_t>> engineGetLocalStorage(
        akkaradb::engine::AkkEngine& engine,
        const std::string& key
    ) {
        const auto keyBytes = bytesOf(key);
        akkaradb::core::BufferArena arena;
        for (const auto& record : engine.scanLocalStorage(arena, keyBytes)) {
            if (!std::ranges::equal(record.key, keyBytes)) { break; }
            return std::vector<uint8_t>{record.value.begin(), record.value.end()};
        }
        return std::nullopt;
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

    std::string keyPlacedOn(const ClusterConfig& cfg, std::span<const uint64_t> nodeIds, const std::string& prefix) {
        ClusterRouter router{cfg};
        for (uint32_t i = 0; i < 10000; ++i) {
            std::string key = prefix + "-" + std::to_string(i);
            const auto targets = router.writeTargets(bytesOf(key));
            if (targets.size() != nodeIds.size()) { continue; }
            bool matches = true;
            for (size_t index = 0; index < targets.size(); ++index) {
                if (targets[index].nodeId != nodeIds[index]) { matches = false; break; }
            }
            if (matches) { return key; }
        }
        throw std::runtime_error("cluster smoke: failed to find STRIPE placement key");
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
            {node(1, 21801, 21901), node(2, 21802, 21902), node(3, 21803, 21903)},
            ReplicationMode::STRIPE, AckPolicy{},
            // STRIPE forces a durable target acknowledgement even in ASYNC mode.
            // Leave enough time for a real Windows FlushFileBuffers cycle under load.
            ConsistencyOptions{.mode = ConsistencyMode::ASYNC, .ackTimeoutMs = 2000},
            {}, StripeOptions{.dataShards = 1, .parityShards = 1},
            0, testClusterId(9180),
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
        options.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
        options.cluster.runtime.stripeReadCoordinatorMode = StripeReadCoordinatorMode::LOCAL_COORDINATOR;
        writeNodeIdFile(options.paths.dataDir / "node.id", id);
        return options;
    }

    int runStripeCrashWriter(const char* point, const std::filesystem::path& dir) {
        auto n1 = akkaradb::engine::AkkEngine::open(stripeStorageOptions(dir, 1));
        auto n2 = akkaradb::engine::AkkEngine::open(stripeStorageOptions(dir, 2));
        auto n3 = akkaradb::engine::AkkEngine::open(stripeStorageOptions(dir, 3));
        constexpr std::array<uint64_t, 2> placement{1, 2};
        const auto key = keyPlacedOn(stripeStorageConfig(), placement, "stripe-crash");
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
        for (const auto& record : engine.scanLocalStorage(arena)) {
            if (record.key.size() < 6 || record.key[0] != 0 || record.key[1] != 'A' || record.key[2] != 'K' || record.key[3] != 'S') { continue; }
            if (record.key[4] == 'S') { ++shards; }
            if (record.key[4] == 'T') { ++intents; }
        }
        return {shards, intents};
    }

    bool hasStripeMetadataMagic(akkaradb::engine::AkkEngine& engine, std::string_view magic) {
        akkaradb::core::BufferArena arena;
        for (const auto& record : engine.scanLocalStorage(arena)) {
            if (record.key.size() >= 6 && record.key[0] == 0 && record.key[1] == 'A' && record.key[2] == 'K' &&
                record.key[3] == 'S' && record.key[4] == 'M' && record.key[5] == '1') {
                return record.value.size() >= magic.size() &&
                    std::equal(magic.begin(), magic.end(), record.value.begin());
            }
        }
        return false;
    }

    void testStripePublicationCrashRecovery(const std::filesystem::path& executable) {
        constexpr std::array<uint64_t, 2> placement{1, 2};
        const auto key = keyPlacedOn(stripeStorageConfig(), placement, "stripe-crash");
        for (const char* point : {"stripe.after_intent", "stripe.after_shard", "stripe.before_publish", "stripe.after_publish"}) {
            const auto dir = makeTempDir(point);
            runStorageCrashChild(executable, "--stripe-crash-writer", point, dir);
            auto n1 = akkaradb::engine::AkkEngine::open(stripeStorageOptions(dir, 1));
            auto n2 = akkaradb::engine::AkkEngine::open(stripeStorageOptions(dir, 2));
            auto n3 = akkaradb::engine::AkkEngine::open(stripeStorageOptions(dir, 3));
            const std::string expected = std::string_view{point} == "stripe.after_publish" ? "after" : "before";
            AKK_CLUSTER_CHECK(waitUntil([&] {
                try {
                    const auto value = engineGet(*n2, key);
                    return value && textOf(*value) == expected;
                }
                catch (const std::runtime_error&) { return false; }
            }));
            const bool intentsRetired = waitUntil([&] {
                const auto a = stripeInternalCounts(*n1);
                const auto b = stripeInternalCounts(*n2);
                return a.first == 1 && b.first == 1 && a.second == 0 && b.second == 0;
            }, std::chrono::milliseconds{20000});
            if (!intentsRetired) {
                const auto a = stripeInternalCounts(*n1);
                const auto b = stripeInternalCounts(*n2);
                throw std::runtime_error(
                    std::string{"STRIPE crash recovery did not retire intents at "} + point +
                    ": n1=" + std::to_string(a.first) + "/" + std::to_string(a.second) +
                    " n2=" + std::to_string(b.first) + "/" + std::to_string(b.second)
                );
            }
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
            n3->close();
        }
    }

    void testStripeFailureAndConcurrentReads() {
        const auto dir = makeTempDir("stripe-failure-and-concurrency");
        constexpr std::array<uint64_t, 2> placement{1, 2};
        const auto key = keyPlacedOn(stripeStorageConfig(), placement, "stripe-failure");
        auto n1 = akkaradb::engine::AkkEngine::open(stripeStorageOptions(dir, 1));
        auto n2 = akkaradb::engine::AkkEngine::open(stripeStorageOptions(dir, 2));
        auto n3 = akkaradb::engine::AkkEngine::open(stripeStorageOptions(dir, 3));
        std::string initialWriteFailure;
        const bool initialWriteReady = waitUntil([&] {
            try { n1->put(bytesOf(key), bytesOf("stable")); return true; }
            catch (const std::runtime_error& error) { initialWriteFailure = error.what(); return false; }
        });
        if (!initialWriteReady) {
            throw std::runtime_error("STRIPE concurrent-read setup write did not become ready: " + initialWriteFailure);
        }
        n2->close();
        n2.reset();
        n1->put(bytesOf(key), bytesOf("degraded-write"));
        const auto previous = engineGet(*n1, key);
        AKK_CLUSTER_CHECK(previous && textOf(*previous) == "degraded-write");
        n2 = akkaradb::engine::AkkEngine::open(stripeStorageOptions(dir, 2));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try {
                const auto value = engineGet(*n2, key);
                return value && textOf(*value) == "degraded-write";
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
                    if (text != "stable" && text != "degraded-write" &&
                        text != std::string(4096, 'a') && text != std::string(4096, 'b')) {
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
        n3->close();
        n1->close();

        auto options = stripeStorageOptions(makeTempDir("stripe-requires-wal"), 1);
        options.components.walEnabled = false;
        bool rejected = false;
        try { (void)akkaradb::engine::AkkEngine::open(options); }
        catch (const std::invalid_argument&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected);
    }

    ClusterConfig raid0StorageConfig() {
        return ClusterConfig{
            {
                node(1, 23801, 23901),
                node(2, 23802, 23902),
                node(3, 23803, 23903),
            },
            RaidOptions{.preset = RaidPreset::RAID0, .dataShards = 3},
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 2000},
            testClusterId(9190),
        };
    }

    akkaradb::engine::AkkEngineOptions raid0StorageOptions(const std::filesystem::path& dir, uint64_t id) {
        akkaradb::engine::AkkEngineOptions options;
        options.paths.dataDir = dir / ("n" + std::to_string(id));
        options.components.clusterEnabled = true;
        options.components.blobEnabled = false;
        options.cluster.config = raid0StorageConfig();
        options.cluster.runtime.transportMode = TransportMode::PLAIN;
        options.cluster.runtime.clusterGroupId = 9190;
        options.cluster.runtime.clusterGroupEpoch = 1;
        options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        options.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
        options.cluster.runtime.stripeReadCoordinatorMode = StripeReadCoordinatorMode::LOCAL_COORDINATOR;
        writeNodeIdFile(options.paths.dataDir / "node.id", id);
        return options;
    }

    void testRaid0PresetStorageAndFailure() {
        const auto dir = makeTempDir("raid0-preset");
        const auto config = raid0StorageConfig();
        AKK_CLUSTER_CHECK(config.mode() == ReplicationMode::STRIPE);
        AKK_CLUSTER_CHECK(config.raidPreset() == RaidPreset::RAID0);
        AKK_CLUSTER_CHECK(config.stripe().dataShards == 3);
        AKK_CLUSTER_CHECK(config.stripe().parityShards == 0);

        const auto configPath = dir / "cluster.cfg";
        ClusterConfig::save(configPath, config);
        {
            std::ifstream file{configPath, std::ios::binary};
            std::array<uint8_t, 6> header{};
            file.read(reinterpret_cast<char*>(header.data()), static_cast<std::streamsize>(header.size()));
            AKK_CLUSTER_CHECK(file.gcount() == static_cast<std::streamsize>(header.size()));
            AKK_CLUSTER_CHECK(std::memcmp(header.data(), "AKC1", 4) == 0);
            AKK_CLUSTER_CHECK(header[4] == 1 && header[5] == 0);
        }
        const auto loaded = ClusterConfig::load(configPath);
        AKK_CLUSTER_CHECK(loaded.mode() == ReplicationMode::STRIPE);
        AKK_CLUSTER_CHECK(loaded.raidPreset() == RaidPreset::RAID0);
        AKK_CLUSTER_CHECK(loaded.stripe().dataShards == 3);
        AKK_CLUSTER_CHECK(loaded.stripe().parityShards == 0);

        bool rejectedSingleShard = false;
        try {
            (void)ClusterConfig{
                {node(1, 23811, 23911)},
                RaidOptions{.preset = RaidPreset::RAID0, .dataShards = 1},
                AckPolicy{},
            };
        }
        catch (const std::invalid_argument&) { rejectedSingleShard = true; }
        AKK_CLUSTER_CHECK(rejectedSingleShard);

        auto n1 = akkaradb::engine::AkkEngine::open(raid0StorageOptions(dir, 1));
        auto n2 = akkaradb::engine::AkkEngine::open(raid0StorageOptions(dir, 2));
        auto n3 = akkaradb::engine::AkkEngine::open(raid0StorageOptions(dir, 3));
        const auto key = keyOwnedBy(config, 1, "raid0-key");
        const auto value = deterministicBytes(4099, 0xA11A'1D00'0000'0001ull);
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try {
                n1->put(bytesOf(key), value);
                return true;
            }
            catch (const std::runtime_error&) { return false; }
        }, std::chrono::milliseconds{10000}));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try {
                const auto stored = engineGet(*n2, key);
                return stored && *stored == value;
            }
            catch (const std::runtime_error&) { return false; }
        }, std::chrono::milliseconds{10000}));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto a = stripeInternalCounts(*n1);
            const auto b = stripeInternalCounts(*n2);
            const auto c = stripeInternalCounts(*n3);
            return a.first == 1 && b.first == 1 && c.first == 1;
        }));

        n3->close();
        n3.reset();
        bool rejectedIncompleteWrite = false;
        try { n1->put(bytesOf(key), bytesOf("must-not-publish")); }
        catch (const std::runtime_error&) { rejectedIncompleteWrite = true; }
        AKK_CLUSTER_CHECK(rejectedIncompleteWrite);
        bool rejectedMissingShard = false;
        try { (void)engineGet(*n2, key); }
        catch (const std::runtime_error&) { rejectedMissingShard = true; }
        AKK_CLUSTER_CHECK(rejectedMissingShard);

        n3 = akkaradb::engine::AkkEngine::open(raid0StorageOptions(dir, 3));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try {
                const auto stored = engineGet(*n2, key);
                return stored && *stored == value;
            }
            catch (const std::runtime_error&) { return false; }
        }, std::chrono::milliseconds{10000}));
        n3->close();
        n2->close();
        n1->close();
    }

    ClusterConfig raid1StorageConfig() {
        return ClusterConfig{
            {
                node(1, 23821, 23921),
                node(2, 23822, 23922),
            },
            RaidOptions{.preset = RaidPreset::RAID1, .primaryNodeId = 1},
            AckPolicy{.mode = AckPolicyMode::NONE, .stage = AckStage::RECEIVED},
            ConsistencyOptions{
                .mode = ConsistencyMode::ASYNC,
                .writeConsistency = WriteConsistency::LOCAL,
                .ackTimeoutAction = AckTimeoutAction::ACCEPT_LOCAL,
                .replicaLagAction = ReplicaLagAction::REJECT_REPLICA,
                .ackTimeoutMs = 2000,
            },
            testClusterId(9191),
        };
    }

    akkaradb::engine::AkkEngineOptions raid1StorageOptions(const std::filesystem::path& dir, uint64_t id) {
        akkaradb::engine::AkkEngineOptions options;
        options.paths.dataDir = dir / ("n" + std::to_string(id));
        options.components.clusterEnabled = true;
        options.components.blobEnabled = false;
        options.cluster.config = raid1StorageConfig();
        options.cluster.runtime.transportMode = TransportMode::PLAIN;
        options.cluster.runtime.startupRole = id == 1 ? NodeStartupRole::PRIMARY : NodeStartupRole::REPLICA;
        writeNodeIdFile(options.paths.dataDir / "node.id", id);
        return options;
    }

    void testRaid1PresetMirroringAndRebuild() {
        const auto dir = makeTempDir("raid1-preset");
        const auto config = raid1StorageConfig();
        AKK_CLUSTER_CHECK(config.mode() == ReplicationMode::MIRROR);
        AKK_CLUSTER_CHECK(config.raidPreset() == RaidPreset::RAID1);
        AKK_CLUSTER_CHECK(config.primaryNodeId() == 1);
        AKK_CLUSTER_CHECK(config.ackPolicy().mode == AckPolicyMode::ALL_TARGETS);
        AKK_CLUSTER_CHECK(config.ackPolicy().stage == AckStage::DURABLE);
        AKK_CLUSTER_CHECK(config.consistency().mode == ConsistencyMode::PRIMARY_ACK);
        AKK_CLUSTER_CHECK(config.consistency().writeConsistency == WriteConsistency::AVAILABLE_REPLICAS);
        AKK_CLUSTER_CHECK(config.consistency().ackTimeoutAction == AckTimeoutAction::FAIL_WRITE);
        AKK_CLUSTER_CHECK(config.consistency().replicaLagAction == ReplicaLagAction::ASYNC_RESYNC);

        const auto configPath = dir / "cluster.cfg";
        ClusterConfig::save(configPath, config);
        const auto loaded = ClusterConfig::load(configPath);
        AKK_CLUSTER_CHECK(loaded.raidPreset() == RaidPreset::RAID1);
        AKK_CLUSTER_CHECK(loaded.primaryNodeId() == 1);

        bool rejectedSingleNode = false;
        try {
            (void)ClusterConfig{
                {node(1, 23823, 23923)},
                RaidOptions{.preset = RaidPreset::RAID1, .primaryNodeId = 1},
                AckPolicy{},
            };
        }
        catch (const std::invalid_argument&) { rejectedSingleNode = true; }
        AKK_CLUSTER_CHECK(rejectedSingleNode);

        auto noWal = raid1StorageOptions(makeTempDir("raid1-requires-wal"), 1);
        noWal.components.walEnabled = false;
        bool rejectedWithoutWal = false;
        try { (void)akkaradb::engine::AkkEngine::open(noWal); }
        catch (const std::invalid_argument&) { rejectedWithoutWal = true; }
        AKK_CLUSTER_CHECK(rejectedWithoutWal);

        auto primary = akkaradb::engine::AkkEngine::open(raid1StorageOptions(dir, 1));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            return primary->stats().cluster.health == akkaradb::engine::ClusterHealthState::DEGRADED;
        }));
        primary->put(bytesOf("raid1-offline"), bytesOf("primary-durable"));

        for (unsigned i = 0; i < 128; ++i) {
            const auto key = "raid1-rebuild-" + std::to_string(i);
            primary->put(bytesOf(key), deterministicBytes(4096, 0xA11A'1D01'0000'0000ull + i));
        }

        auto replica = akkaradb::engine::AkkEngine::open(raid1StorageOptions(dir, 2));
        const bool observedRebuilding = waitUntil([&] {
            return primary->stats().cluster.health == akkaradb::engine::ClusterHealthState::REBUILDING;
        }, std::chrono::milliseconds{10000});
        AKK_CLUSTER_CHECK(observedRebuilding);
        AKK_CLUSTER_CHECK(waitUntil([&] {
            return primary->stats().cluster.health == akkaradb::engine::ClusterHealthState::HEALTHY;
        }, std::chrono::milliseconds{20000}));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto value = engineGet(*replica, "raid1-offline");
            return value && textOf(*value) == "primary-durable";
        }, std::chrono::milliseconds{10000}));

        primary->put(bytesOf("raid1-live"), bytesOf("both-durable"));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto value = engineGet(*replica, "raid1-live");
            return value && textOf(*value) == "both-durable";
        }));

        replica->close();
        replica.reset();
        AKK_CLUSTER_CHECK(waitUntil([&] {
            return primary->stats().cluster.health == akkaradb::engine::ClusterHealthState::DEGRADED;
        }));
        primary->put(bytesOf("raid1-degraded"), bytesOf("survives-rebuild"));

        replica = akkaradb::engine::AkkEngine::open(raid1StorageOptions(dir, 2));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            return primary->stats().cluster.health == akkaradb::engine::ClusterHealthState::HEALTHY;
        }, std::chrono::milliseconds{20000}));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto value = engineGet(*replica, "raid1-degraded");
            return value && textOf(*value) == "survives-rebuild";
        }));

        primary->close();
        primary.reset();
        bool replicaRejectedWrite = false;
        try { replica->put(bytesOf("no-auto-promotion"), bytesOf("rejected")); }
        catch (const std::runtime_error&) { replicaRejectedWrite = true; }
        AKK_CLUSTER_CHECK(replicaRejectedWrite);
        replica->close();
    }

    ClusterConfig raid5StorageConfig() {
        return ClusterConfig{
            {
                node(1, 23831, 23931),
                node(2, 23832, 23932),
                node(3, 23833, 23933),
            },
            RaidOptions{.preset = RaidPreset::RAID5, .dataShards = 2},
            AckPolicy{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::APPLIED},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 250},
            testClusterId(9192),
        };
    }

    akkaradb::engine::AkkEngineOptions raid5StorageOptions(const std::filesystem::path& dir, uint64_t id) {
        akkaradb::engine::AkkEngineOptions options;
        options.paths.dataDir = dir / ("n" + std::to_string(id));
        options.components.clusterEnabled = true;
        options.components.blobEnabled = false;
        options.cluster.config = raid5StorageConfig();
        options.cluster.runtime.transportMode = TransportMode::PLAIN;
        options.cluster.runtime.clusterGroupId = 9192;
        options.cluster.runtime.clusterGroupEpoch = 1;
        options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        options.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
        options.cluster.runtime.stripeReadCoordinatorMode = StripeReadCoordinatorMode::LOCAL_COORDINATOR;
        writeNodeIdFile(options.paths.dataDir / "node.id", id);
        return options;
    }

    void testRaid5PresetDegradedWriteAndRead() {
        const auto dir = makeTempDir("raid5-preset");
        const auto config = raid5StorageConfig();
        AKK_CLUSTER_CHECK(config.mode() == ReplicationMode::STRIPE);
        AKK_CLUSTER_CHECK(config.raidPreset() == RaidPreset::RAID5);
        AKK_CLUSTER_CHECK(config.stripe().dataShards == 2);
        AKK_CLUSTER_CHECK(config.stripe().parityShards == 1);

        const auto configPath = dir / "cluster.cfg";
        ClusterConfig::save(configPath, config);
        const auto loaded = ClusterConfig::load(configPath);
        AKK_CLUSTER_CHECK(loaded.raidPreset() == RaidPreset::RAID5);
        AKK_CLUSTER_CHECK(loaded.stripe().dataShards == 2);
        AKK_CLUSTER_CHECK(loaded.stripe().parityShards == 1);

        bool rejectedSingleDataShard = false;
        try {
            (void)ClusterConfig{
                {node(1, 23834, 23934), node(2, 23835, 23935), node(3, 23836, 23936)},
                RaidOptions{.preset = RaidPreset::RAID5, .dataShards = 1},
                AckPolicy{},
            };
        }
        catch (const std::invalid_argument&) { rejectedSingleDataShard = true; }
        AKK_CLUSTER_CHECK(rejectedSingleDataShard);

        constexpr std::array<uint64_t, 3> placement{1, 2, 3};
        const auto key = keyPlacedOn(config, placement, "raid5-degraded");
        auto n1 = akkaradb::engine::AkkEngine::open(raid5StorageOptions(dir, 1));
        auto n2 = akkaradb::engine::AkkEngine::open(raid5StorageOptions(dir, 2));
        auto n3 = akkaradb::engine::AkkEngine::open(raid5StorageOptions(dir, 3));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { n1->put(bytesOf(key), bytesOf("raid5-full")); return true; }
            catch (const std::runtime_error&) { return false; }
        }, std::chrono::milliseconds{15000}));

        n3->close();
        n3.reset();
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto stats = n1->stats();
            const auto peer = std::ranges::find(
                stats.cluster.peers, uint64_t{3}, &akkaradb::engine::EngineStats::ClusterStats::PeerStats::nodeId
            );
            return peer != stats.cluster.peers.end() && !peer->connected;
        }));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { n1->put(bytesOf(key), bytesOf("raid5-one-node-down")); return true; }
            catch (const std::runtime_error&) { return false; }
        }, std::chrono::milliseconds{15000}));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try {
                const auto value = engineGet(*n2, key);
                return value && textOf(*value) == "raid5-one-node-down";
            }
            catch (const std::runtime_error&) { return false; }
        }, std::chrono::milliseconds{15000}));

        n1->close();
        n2->close();
    }

    void testStripeDistributedRollback() {
        namespace engine = akkaradb::engine;
        const auto dir = makeTempDir("stripe-distributed-rollback");
        auto optionsFor = [&](uint64_t id) {
            auto options = raid5StorageOptions(dir, id);
            options.components.versionLogEnabled = true;
            return options;
        };
        auto n1 = engine::AkkEngine::open(optionsFor(1));
        auto n2 = engine::AkkEngine::open(optionsFor(2));
        auto n3 = engine::AkkEngine::open(optionsFor(3));
        const auto config = raid5StorageConfig();
        const std::string key = keyOwnedBy(config, 1, "stripe-rollback");
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { n1->put(bytesOf(key), bytesOf("stripe-old")); return true; }
            catch (...) { return false; }
        }, std::chrono::milliseconds{15000}));
        const auto checkpoint = n2->createClusterCheckpoint();
        n1->put(bytesOf(key), bytesOf("stripe-new"));
        const auto rollback = n3->rollbackTo(checkpoint);
        AKK_CLUSTER_CHECK(rollback.complete());
        AKK_CLUSTER_CHECK(rollback.appliedCount() == 1);
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try {
                const auto value = engineGet(*n2, key);
                return value && textOf(*value) == "stripe-old";
            }
            catch (...) { return false; }
        }, std::chrono::milliseconds{15000}));
        n3->close();
        n2->close();
        n1->close();
    }

    void testGlobalStreamDistributedRollback() {
        namespace engine = akkaradb::engine;
        const auto run = [&](bool raft) {
            const auto dir = makeTempDir(raft ? "raft-global-rollback" : "mirror-global-rollback");
            const uint16_t basePort = raft ? 21751 : 21741;
            std::vector<NodeInfo> nodes{node(1, static_cast<uint16_t>(basePort - 100), basePort)};
            if (!raft) { nodes.push_back(node(2, static_cast<uint16_t>(basePort - 99), static_cast<uint16_t>(basePort + 1))); }
            const ClusterConfig cfg{
                nodes,
                ReplicationMode::MIRROR,
                AckPolicy{},
                ConsistencyOptions{
                    .mode = raft ? ConsistencyMode::RAFT_QUORUM : ConsistencyMode::PRIMARY_ACK,
                    .ackTimeoutMs = 1000,
                },
                {},
                {},
                raft ? 0ULL : 1ULL,
                testClusterId(raft ? 9005 : 9004),
            };
            const auto makeOptions = [&](uint64_t nodeId) {
                engine::AkkEngineOptions options;
                options.paths.dataDir = dir / ("n" + std::to_string(nodeId));
                options.components.clusterEnabled = true;
                options.components.blobEnabled = false;
                options.components.versionLogEnabled = true;
                options.cluster.config = cfg;
                options.cluster.runtime.transportMode = TransportMode::PLAIN;
                options.cluster.runtime.clusterGroupId = raft ? 9005 : 9004;
                options.cluster.runtime.clusterGroupEpoch = 1;
                options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
                options.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
                options.cluster.runtime.startupRole = nodeId == 1 ? NodeStartupRole::PRIMARY : NodeStartupRole::REPLICA;
                writeNodeIdFile(options.paths.dataDir / "node.id", nodeId);
                return options;
            };
            auto database = engine::AkkEngine::open(makeOptions(1));
            auto replica = raft ? std::unique_ptr<engine::AkkEngine>{} : engine::AkkEngine::open(makeOptions(2));
            if (!waitUntil([&] {
                return database->stats().cluster.role == static_cast<uint32_t>(NodeRole::PRIMARY);
            }, std::chrono::milliseconds{8000})) {
                throw std::runtime_error(raft ? "RAFT rollback test did not elect a leader" : "MIRROR rollback test did not start its primary");
            }
            database->put(bytesOf("global-rollback"), bytesOf("old"));
            const auto checkpoint = database->createClusterCheckpoint();
            AKK_CLUSTER_CHECK(checkpoint.watermarks.size() == 1 && checkpoint.watermarks.front().streamId == 0);
            database->put(bytesOf("global-rollback"), bytesOf("new"));
            const auto all = database->rollbackTo(checkpoint);
            AKK_CLUSTER_CHECK(all.complete() && all.appliedCount() == 1);
            AKK_CLUSTER_CHECK(textOf(*engineGet(*database, "global-rollback")) == "old");
            database->put(bytesOf("global-rollback"), bytesOf("newer"));
            const auto one = database->rollbackKey(bytesOf("global-rollback"), checkpoint.watermarks.front());
            AKK_CLUSTER_CHECK(one.complete() && one.appliedCount() == 1);
            AKK_CLUSTER_CHECK(textOf(*engineGet(*database, "global-rollback")) == "old");
            AKK_CLUSTER_CHECK(database->stats().vlog.rollbackEntries >= 2);
            if (replica) {
                AKK_CLUSTER_CHECK(waitUntil([&] {
                    const auto value = engineGetLocalStorage(*replica, "global-rollback");
                    return value && textOf(*value) == "old";
                }));
                replica->close();
            }
            database->close();
        };
        run(false);
        run(true);
    }

    ClusterConfig raid10StorageConfig() {
        return ClusterConfig{
            {
                node(1, 23841, 23941),
                node(2, 23842, 23942),
                node(3, 23843, 23943),
                node(4, 23844, 23944),
            },
            RaidOptions{.preset = RaidPreset::RAID10, .dataShards = 2},
            AckPolicy{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::APPLIED},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 250},
            testClusterId(9193),
        };
    }

    akkaradb::engine::AkkEngineOptions raid10StorageOptions(const std::filesystem::path& dir, uint64_t id) {
        akkaradb::engine::AkkEngineOptions options;
        options.paths.dataDir = dir / ("n" + std::to_string(id));
        options.components.clusterEnabled = true;
        options.components.blobEnabled = false;
        options.cluster.config = raid10StorageConfig();
        options.cluster.runtime.transportMode = TransportMode::PLAIN;
        options.cluster.runtime.clusterGroupId = 9193;
        options.cluster.runtime.clusterGroupEpoch = 1;
        options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        options.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
        options.cluster.runtime.stripeReadCoordinatorMode = StripeReadCoordinatorMode::LOCAL_COORDINATOR;
        options.cluster.runtime.stripeRebuildIntervalMs = 100;
        writeNodeIdFile(options.paths.dataDir / "node.id", id);
        return options;
    }

    void testRaid10PresetMirroredStripeAndRebuild() {
        const auto dir = makeTempDir("raid10-preset");
        const auto config = raid10StorageConfig();
        AKK_CLUSTER_CHECK(config.mode() == ReplicationMode::STRIPE);
        AKK_CLUSTER_CHECK(config.raidPreset() == RaidPreset::RAID10);
        AKK_CLUSTER_CHECK(config.stripe().dataShards == 2);
        AKK_CLUSTER_CHECK(config.stripe().parityShards == 0);
        AKK_CLUSTER_CHECK(config.stripe().copiesPerShard == 2);
        AKK_CLUSTER_CHECK(config.stripe().totalPlacements() == 4);

        const auto configPath = dir / "cluster.cfg";
        ClusterConfig::save(configPath, config);
        const auto loaded = ClusterConfig::load(configPath);
        AKK_CLUSTER_CHECK(loaded.raidPreset() == RaidPreset::RAID10);
        AKK_CLUSTER_CHECK(loaded.stripe().copiesPerShard == 2);

        bool rejectedSinglePair = false;
        try {
            (void)ClusterConfig{
                {node(1, 23845, 23945), node(2, 23846, 23946)},
                RaidOptions{.preset = RaidPreset::RAID10, .dataShards = 1},
                AckPolicy{},
            };
        }
        catch (const std::invalid_argument&) { rejectedSinglePair = true; }
        AKK_CLUSTER_CHECK(rejectedSinglePair);

        bool rejectedUnpairedNode = false;
        try {
            (void)ClusterConfig{
                {
                    node(1, 23845, 23945),
                    node(2, 23846, 23946),
                    node(3, 23847, 23947),
                    node(4, 23848, 23948),
                    node(5, 23849, 23949),
                },
                RaidOptions{.preset = RaidPreset::RAID10, .dataShards = 2},
                AckPolicy{},
            };
        }
        catch (const std::invalid_argument&) { rejectedUnpairedNode = true; }
        AKK_CLUSTER_CHECK(rejectedUnpairedNode);

        const ClusterConfig hotSpareConfig{
            {
                node(1, 23845, 23945),
                node(2, 23846, 23946),
                node(3, 23847, 23947),
                node(4, 23848, 23948),
                node(5, 23849, 23949),
            },
            RaidOptions{.preset = RaidPreset::RAID10, .dataShards = 2, .hotSpareNodeId = 5},
            AckPolicy{},
        };
        AKK_CLUSTER_CHECK(hotSpareConfig.stripeFailoverNode() != nullptr);
        AKK_CLUSTER_CHECK(hotSpareConfig.stripeFailoverNode()->nodeId == 5);
        AKK_CLUSTER_CHECK(hotSpareConfig.stripePlacementNodes().size() == 4);
        const auto hotSpareTargets = ClusterRouter{hotSpareConfig}.stripeShardTargets(bytesOf("raid10-hot-spare-layout"));
        AKK_CLUSTER_CHECK(std::ranges::none_of(hotSpareTargets, [](const auto& target) { return target.node.nodeId == 5; }));

        bool rejectedSpareWithoutFullPairs = false;
        try {
            (void)ClusterConfig{
                {
                    node(1, 23845, 23945),
                    node(2, 23846, 23946),
                    node(3, 23847, 23947),
                    node(4, 23848, 23948),
                },
                RaidOptions{.preset = RaidPreset::RAID10, .dataShards = 2, .hotSpareNodeId = 4},
                AckPolicy{},
            };
        }
        catch (const std::invalid_argument&) { rejectedSpareWithoutFullPairs = true; }
        AKK_CLUSTER_CHECK(rejectedSpareWithoutFullPairs);

        auto n1 = akkaradb::engine::AkkEngine::open(raid10StorageOptions(dir, 1));
        auto n2 = akkaradb::engine::AkkEngine::open(raid10StorageOptions(dir, 2));
        auto n3 = akkaradb::engine::AkkEngine::open(raid10StorageOptions(dir, 3));
        auto n4 = akkaradb::engine::AkkEngine::open(raid10StorageOptions(dir, 4));
        uint64_t metadataLeader = 0;
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto a = n1->stats().cluster.stripeMetadataLeaderNodeId;
            const auto b = n2->stats().cluster.stripeMetadataLeaderNodeId;
            const auto c = n3->stats().cluster.stripeMetadataLeaderNodeId;
            const auto d = n4->stats().cluster.stripeMetadataLeaderNodeId;
            if (a == 0 || a != b || a != c || a != d) { return false; }
            metadataLeader = a;
            return true;
        }, std::chrono::milliseconds{15000}));

        const bool leaderInFirstPair = metadataLeader == 1 || metadataLeader == 2;
        const std::array<uint64_t, 4> placement = leaderInFirstPair
            ? std::array<uint64_t, 4>{3, 4, 1, 2}
            : std::array<uint64_t, 4>{1, 2, 3, 4};
        const auto key = keyPlacedOn(config, placement, "raid10-degraded");
        ClusterRouter router{config};
        const auto targets = router.stripeShardTargets(bytesOf(key));
        AKK_CLUSTER_CHECK(targets.size() == 4);
        AKK_CLUSTER_CHECK(targets[0].shardIndex == 0 && targets[0].replicaIndex == 0);
        AKK_CLUSTER_CHECK(targets[1].shardIndex == 0 && targets[1].replicaIndex == 1);
        AKK_CLUSTER_CHECK(targets[2].shardIndex == 1 && targets[2].replicaIndex == 0);
        AKK_CLUSTER_CHECK(targets[3].shardIndex == 1 && targets[3].replicaIndex == 1);
        for (size_t i = 0; i < placement.size(); ++i) { AKK_CLUSTER_CHECK(targets[i].node.nodeId == placement[i]); }

        auto* owner = leaderInFirstPair ? n3.get() : n1.get();
        const uint64_t mirrorNodeId = leaderInFirstPair ? 4 : 2;
        AKK_CLUSTER_CHECK(owner->stats().nodeId != metadataLeader);
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { owner->put(bytesOf(key), bytesOf("raid10-full")); return true; }
            catch (const std::runtime_error&) { return false; }
        }, std::chrono::milliseconds{15000}));

        if (mirrorNodeId == 2) { n2->close(); n2.reset(); }
        else { n4->close(); n4.reset(); }
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto stats = owner->stats();
            const auto peer = std::ranges::find(
                stats.cluster.peers, mirrorNodeId, &akkaradb::engine::EngineStats::ClusterStats::PeerStats::nodeId
            );
            return peer != stats.cluster.peers.end() && !peer->connected;
        }));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { owner->put(bytesOf(key), bytesOf("raid10-one-mirror-down")); return true; }
            catch (const std::runtime_error&) { return false; }
        }, std::chrono::milliseconds{15000}));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try {
                const auto value = engineGet(*owner, key);
                return value && textOf(*value) == "raid10-one-mirror-down";
            }
            catch (const std::runtime_error&) { return false; }
        }, std::chrono::milliseconds{15000}));

        if (mirrorNodeId == 2) { n2 = akkaradb::engine::AkkEngine::open(raid10StorageOptions(dir, 2)); }
        else { n4 = akkaradb::engine::AkkEngine::open(raid10StorageOptions(dir, 4)); }
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto a = n1->stats().stripeRebuild.succeeded;
            const auto b = n2->stats().stripeRebuild.succeeded;
            const auto c = n3->stats().stripeRebuild.succeeded;
            const auto d = n4->stats().stripeRebuild.succeeded;
            return a + b + c + d >= 1;
        }, std::chrono::milliseconds{15000}));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try {
                auto* rebuilt = mirrorNodeId == 2 ? n2.get() : n4.get();
                const auto value = engineGet(*rebuilt, key);
                return value && textOf(*value) == "raid10-one-mirror-down";
            }
            catch (const std::runtime_error&) { return false; }
        }, std::chrono::milliseconds{15000}));

        n4->close();
        n3->close();
        n2->close();
        n1->close();
    }

    ClusterConfig raid10HotSpareStorageConfig() {
        return ClusterConfig{
            {
                node(1, 23851, 23951),
                node(2, 23852, 23952),
                node(3, 23853, 23953),
                node(4, 23854, 23954),
                node(5, 23855, 23955),
            },
            RaidOptions{.preset = RaidPreset::RAID10, .dataShards = 2, .hotSpareNodeId = 5},
            AckPolicy{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::APPLIED},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 250},
            testClusterId(9194),
        };
    }

    akkaradb::engine::AkkEngineOptions raid10HotSpareStorageOptions(const std::filesystem::path& dir, uint64_t id) {
        auto options = raid10StorageOptions(dir, id);
        options.cluster.config = raid10HotSpareStorageConfig();
        options.cluster.runtime.clusterGroupId = 9194;
        return options;
    }

    void testRaid10HotSpareReplacementAndNoFailback() {
        const auto dir = makeTempDir("raid10-hot-spare");
        const auto config = raid10HotSpareStorageConfig();
        AKK_CLUSTER_CHECK(config.raidPreset() == RaidPreset::RAID10);
        AKK_CLUSTER_CHECK(config.stripeFailoverNode() != nullptr && config.stripeFailoverNode()->nodeId == 5);
        AKK_CLUSTER_CHECK(config.dataNodes().size() == 5);
        AKK_CLUSTER_CHECK(config.stripePlacementNodes().size() == 4);

        const auto configPath = dir / "cluster.cfg";
        ClusterConfig::save(configPath, config);
        const auto loaded = ClusterConfig::load(configPath);
        AKK_CLUSTER_CHECK(loaded.stripeFailoverNode() != nullptr && loaded.stripeFailoverNode()->nodeId == 5);

        constexpr std::array<uint64_t, 4> secondaryFailurePlacement{1, 2, 3, 4};
        constexpr std::array<uint64_t, 4> ownerFailurePlacement{2, 1, 3, 4};
        const auto secondaryFailureKey = keyPlacedOn(config, secondaryFailurePlacement, "raid10-spare-secondary");
        const auto ownerFailureKey = keyPlacedOn(config, ownerFailurePlacement, "raid10-spare-owner");

        auto n1 = akkaradb::engine::AkkEngine::open(raid10HotSpareStorageOptions(dir, 1));
        auto n2 = akkaradb::engine::AkkEngine::open(raid10HotSpareStorageOptions(dir, 2));
        auto n3 = akkaradb::engine::AkkEngine::open(raid10HotSpareStorageOptions(dir, 3));
        auto n4 = akkaradb::engine::AkkEngine::open(raid10HotSpareStorageOptions(dir, 4));
        auto n5 = akkaradb::engine::AkkEngine::open(raid10HotSpareStorageOptions(dir, 5));

        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { n1->put(bytesOf(secondaryFailureKey), bytesOf("before-spare")); return true; }
            catch (const std::runtime_error&) { return false; }
        }, std::chrono::milliseconds{15000}));

        n2->close();
        n2.reset();
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto stats = n1->stats();
            const auto peer = std::ranges::find(
                stats.cluster.peers, uint64_t{2}, &akkaradb::engine::EngineStats::ClusterStats::PeerStats::nodeId
            );
            return peer != stats.cluster.peers.end() && !peer->connected;
        }));

        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { n1->put(bytesOf(secondaryFailureKey), bytesOf("rebuilt-on-spare")); return true; }
            catch (const std::runtime_error&) { return false; }
        }, std::chrono::milliseconds{15000}));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto totalRebuilt = n1->stats().stripeRebuild.succeeded + n3->stats().stripeRebuild.succeeded +
                n4->stats().stripeRebuild.succeeded + n5->stats().stripeRebuild.succeeded;
            return totalRebuilt >= 1 && stripeInternalCounts(*n5).first >= 1;
        }, std::chrono::milliseconds{20000}));

        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { n5->put(bytesOf(ownerFailureKey), bytesOf("owner-on-spare")); return true; }
            catch (const std::runtime_error&) { return false; }
        }, std::chrono::milliseconds{15000}));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try {
                const auto value = engineGet(*n3, ownerFailureKey);
                return value && textOf(*value) == "owner-on-spare";
            }
            catch (const std::runtime_error&) { return false; }
        }, std::chrono::milliseconds{15000}));

        n2 = akkaradb::engine::AkkEngine::open(raid10HotSpareStorageOptions(dir, 2));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { n2->put(bytesOf(ownerFailureKey), bytesOf("no-automatic-failback")); return true; }
            catch (const std::runtime_error&) { return false; }
        }, std::chrono::milliseconds{15000}));

        n2->close();
        n2.reset();
        n1->close();
        n1.reset();
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto stats = n3->stats();
            return std::ranges::count_if(stats.cluster.peers, [](const auto& peer) { return !peer.connected; }) >= 2;
        }));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try {
                const auto value = engineGet(*n3, ownerFailureKey);
                return value && textOf(*value) == "no-automatic-failback";
            }
            catch (const std::runtime_error&) { return false; }
        }, std::chrono::milliseconds{15000}));

        n5->close();
        n4->close();
        n3->close();
        n5.reset();
        n4.reset();
        n3.reset();

        n3 = akkaradb::engine::AkkEngine::open(raid10HotSpareStorageOptions(dir, 3));
        n4 = akkaradb::engine::AkkEngine::open(raid10HotSpareStorageOptions(dir, 4));
        n5 = akkaradb::engine::AkkEngine::open(raid10HotSpareStorageOptions(dir, 5));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try {
                const auto value = engineGet(*n4, ownerFailureKey);
                return value && textOf(*value) == "no-automatic-failback";
            }
            catch (const std::runtime_error&) { return false; }
        }, std::chrono::milliseconds{15000}));

        n5->close();
        n4->close();
        n3->close();
    }

    ClusterConfig stripeFailoverStorageConfig() {
        constexpr uint32_t failoverCapabilities = DATA_AND_COORDINATOR |
            static_cast<uint32_t>(NodeCapability::STRIPE_FAILOVER_ELIGIBLE);
        return ClusterConfig{
            {node(1, 21821, 21921), node(2, 21822, 21922), node(3, 21823, 21923, failoverCapabilities)},
            ReplicationMode::STRIPE, AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 2000},
            {}, StripeOptions{.dataShards = 1, .parityShards = 1},
            0, testClusterId(9181),
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
            const auto stats = owner->stats();
            const auto peer = std::ranges::find(stats.cluster.peers, uint64_t{3}, &akkaradb::engine::EngineStats::ClusterStats::PeerStats::nodeId);
            return peer != stats.cluster.peers.end() && peer->connected;
        }, std::chrono::milliseconds{15000}));
        std::string returnedOwnerError;
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { owner->put(bytesOf(key), bytesOf("owner-returned")); return true; }
            catch (const std::runtime_error& error) { returnedOwnerError = error.what(); return false; }
        }, std::chrono::milliseconds{10000}) ||
            (std::fprintf(stderr, "stripe returned owner write: %s\n", returnedOwnerError.c_str()), false));
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
        callbacks.apply = [](uint64_t, ReplOpType, std::span<const uint8_t>, std::span<const uint8_t>, uint8_t, uint64_t, uint64_t) {};
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
        const std::string oversizedPayload(ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE, 'o');
        bool oversizedRejected = false;
        try { runtime->shipEntry(1, ReplOpType::PUT, bytesOf("oversized"), bytesOf(oversizedPayload), 0, 1); }
        catch (const std::invalid_argument&) { oversizedRejected = true; }
        AKK_CLUSTER_CHECK(oversizedRejected);
        const std::string payload(3 * 1024 * 1024, 'x');
        for (uint64_t seq = 1; seq <= 6; ++seq) {
            runtime->shipEntry(seq, ReplOpType::PUT, bytesOf("large"), bytesOf(payload), 0, 1);
        }
        const auto firstSegment = rotateDir / "cluster-raft.log.segments" / "segment-1.akrl";
        const auto firstBytes = readTestFile(firstSegment);
        const auto modified = std::filesystem::last_write_time(firstSegment);
        runtime->shipEntry(7, ReplOpType::PUT, bytesOf("small"), bytesOf("tail"), 0, 1);
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
        AKK_CLUSTER_CHECK(ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE == 4u * 1024u * 1024u);
        AKK_CLUSTER_CHECK(ClusterRuntimeOptions{}.raftMaxReceiveMemoryBytes == 64ull * 1024 * 1024);
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

        const ClusterConfig localCompletion{cfg.nodes(), ReplicationMode::MIRROR, {},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK}, onlineRaftMembership()};
        AKK_CLUSTER_CHECK(localCompletion.usesDataConsensus() && localCompletion.failover() == FailoverPolicy::NONE);
        AKK_CLUSTER_CHECK(ClusterConfig::decode(localCompletion.encode()).failover() == FailoverPolicy::NONE);

        bool rejected = false;
        try {
            (void)ClusterConfig{
                {
                    node(1, 20154, 20254),
                    node(2, 20155, 20255),
                },
                ReplicationMode::MIRROR,
                AckPolicy{},
                ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .writeConsistency = WriteConsistency::ONE_REPLICA},
                onlineRaftMembership(),
            };
        }
        catch (const std::invalid_argument&) {
            rejected = true;
        }
        AKK_CLUSTER_CHECK(rejected);

        const auto rejectsRuntimeOptions = [&](ClusterRuntimeOptions options) {
            options.transportMode = TransportMode::PLAIN;
            try {
                cfg.validateRuntime(1, options);
                return false;
            }
            catch (const std::invalid_argument&) { return true; }
        };
        ClusterRuntimeOptions invalidEntries;
        invalidEntries.raftSnapshot.minLogEntries = 0;
        AKK_CLUSTER_CHECK(rejectsRuntimeOptions(invalidEntries));
        ClusterRuntimeOptions invalidBytes;
        invalidBytes.raftSnapshot.minLogBytes = 0;
        AKK_CLUSTER_CHECK(rejectsRuntimeOptions(invalidBytes));
        ClusterRuntimeOptions invalidInterval;
        invalidInterval.raftSnapshot.maxIntervalMs = 0;
        AKK_CLUSTER_CHECK(rejectsRuntimeOptions(invalidInterval));
        ClusterRuntimeOptions minimumHeartbeat;
        minimumHeartbeat.raftHeartbeatIntervalMs = 10;
        AKK_CLUSTER_CHECK(!rejectsRuntimeOptions(minimumHeartbeat));
        ClusterRuntimeOptions maximumHeartbeat;
        maximumHeartbeat.raftHeartbeatIntervalMs = 1'000;
        AKK_CLUSTER_CHECK(!rejectsRuntimeOptions(maximumHeartbeat));
        ClusterRuntimeOptions tooFrequentHeartbeat;
        tooFrequentHeartbeat.raftHeartbeatIntervalMs = 9;
        AKK_CLUSTER_CHECK(rejectsRuntimeOptions(tooFrequentHeartbeat));
        ClusterRuntimeOptions tooInfrequentHeartbeat;
        tooInfrequentHeartbeat.raftHeartbeatIntervalMs = 1'001;
        AKK_CLUSTER_CHECK(rejectsRuntimeOptions(tooInfrequentHeartbeat));
        constexpr uint64_t minimumRaftReceiveMemory =
            3ull * (ReplFrameHeader::SIZE + ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE);
        ClusterRuntimeOptions minimumReceiveMemory;
        minimumReceiveMemory.raftMaxReceiveMemoryBytes = minimumRaftReceiveMemory;
        AKK_CLUSTER_CHECK(!rejectsRuntimeOptions(minimumReceiveMemory));
        ClusterRuntimeOptions insufficientReceiveMemory;
        insufficientReceiveMemory.raftMaxReceiveMemoryBytes = minimumRaftReceiveMemory - 1;
        AKK_CLUSTER_CHECK(rejectsRuntimeOptions(insufficientReceiveMemory));
        ClusterRuntimeOptions maximumRequestCapacity;
        maximumRequestCapacity.requests.maxTrackedRequests = 32'768;
        AKK_CLUSTER_CHECK(!rejectsRuntimeOptions(maximumRequestCapacity));
        ClusterRuntimeOptions excessiveRequestCapacity;
        excessiveRequestCapacity.requests.maxTrackedRequests = 32'769;
        AKK_CLUSTER_CHECK(rejectsRuntimeOptions(excessiveRequestCapacity));
        ClusterRuntimeOptions invalidMemorySnapshotBytes;
        invalidMemorySnapshotBytes.memoryOnlySnapshot.maxPinnedBytes = 1024;
        AKK_CLUSTER_CHECK(rejectsRuntimeOptions(invalidMemorySnapshotBytes));
        ClusterRuntimeOptions invalidMemorySnapshotGenerations;
        invalidMemorySnapshotGenerations.memoryOnlySnapshot.maxPinnedGenerations = 0;
        AKK_CLUSTER_CHECK(rejectsRuntimeOptions(invalidMemorySnapshotGenerations));
        ClusterRuntimeOptions throughputSnapshot;
        throughputSnapshot.memoryOnlySnapshot.mode = MemoryOnlySnapshotMode::THROUGHPUT_FIRST;
        AKK_CLUSTER_CHECK(!rejectsRuntimeOptions(throughputSnapshot));
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
            .clusterId = testClusterId(9533),
            .configFingerprint = {1, 2, 3, 4},
        };
        DecodedFrame clientFrame;
        AKK_CLUSTER_CHECK(decodeFrame(encodeClientHello(client), clientFrame));
        ClientHello decodedClient;
        AKK_CLUSTER_CHECK(decodeClientHello(clientFrame.payload, decodedClient));
        AKK_CLUSTER_CHECK(decodedClient.nodeId == client.nodeId);
        AKK_CLUSTER_CHECK(decodedClient.lastSeq == client.lastSeq);
        AKK_CLUSTER_CHECK(decodedClient.groupId == client.groupId);
        AKK_CLUSTER_CHECK(decodedClient.groupEpoch == client.groupEpoch);
        AKK_CLUSTER_CHECK(decodedClient.clusterId == client.clusterId);
        AKK_CLUSTER_CHECK(decodedClient.configFingerprint == client.configFingerprint);
        AKK_CLUSTER_CHECK(!decodeClientHello(std::span<const uint8_t>{clientFrame.payload}.first(34), decodedClient));
        clientFrame.payload.push_back(0);
        AKK_CLUSTER_CHECK(!decodeClientHello(clientFrame.payload, decodedClient));

        const ServerHello server{
            .nodeId = 1,
            .currentSeq = 43,
            .role = NodeRole::PRIMARY,
            .groupId = client.groupId,
            .groupEpoch = client.groupEpoch,
            .clusterId = client.clusterId,
            .configFingerprint = client.configFingerprint,
        };
        DecodedFrame serverFrame;
        AKK_CLUSTER_CHECK(decodeFrame(encodeServerHello(server), serverFrame));
        ServerHello decodedServer;
        AKK_CLUSTER_CHECK(decodeServerHello(serverFrame.payload, decodedServer));
        AKK_CLUSTER_CHECK(decodedServer.nodeId == server.nodeId);
        AKK_CLUSTER_CHECK(decodedServer.currentSeq == server.currentSeq);
        AKK_CLUSTER_CHECK(decodedServer.groupId == server.groupId);
        AKK_CLUSTER_CHECK(decodedServer.groupEpoch == server.groupEpoch);
        AKK_CLUSTER_CHECK(decodedServer.clusterId == server.clusterId);
        AKK_CLUSTER_CHECK(decodedServer.configFingerprint == server.configFingerprint);
        AKK_CLUSTER_CHECK(!decodeServerHello(std::span<const uint8_t>{serverFrame.payload}.first(34), decodedServer));
        serverFrame.payload.push_back(0);
        AKK_CLUSTER_CHECK(!decodeServerHello(serverFrame.payload, decodedServer));
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
        AKK_CLUSTER_CHECK(partitionedTargets.size() == 3);

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

        bool rejectedRaid6WithoutMetadataQuorum = false;
        try {
            (void)ClusterConfig{
                {
                    node(1, 20317, 20417),
                    node(2, 20318, 20418),
                    node(3, 20319, 20419),
                    node(4, 20320, 20420),
                },
                ReplicationMode::STRIPE, AckPolicy{}, {}, {}, StripeOptions{.dataShards = 2, .parityShards = 2},
            };
        }
        catch (const std::invalid_argument&) { rejectedRaid6WithoutMetadataQuorum = true; }
        AKK_CLUSTER_CHECK(rejectedRaid6WithoutMetadataQuorum);

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

    void testForwardFraming() {
        ForwardRequest request;
        request.requestId = 42; request.operation = ForwardOperation::PUT_REQUEST; request.timeoutMs = 500;
        request.deduplicationId.nonce[0] = 1; request.deduplicationId.expiresAtUnixMs = 123;
        request.entries.push_back({{0, 1, 2}, {3, 4}});
        DecodedFrame frame;
        ForwardRequest decoded;
        AKK_CLUSTER_CHECK(decodeFrame(encodeForwardRequest(request), frame));
        AKK_CLUSTER_CHECK(decodeForwardRequest(frame.payload, decoded));
        AKK_CLUSTER_CHECK(decoded.requestId == 42 && decoded.deduplicationId == request.deduplicationId && decoded.entries[0].value == request.entries[0].value);
        frame.payload.push_back(0); AKK_CLUSTER_CHECK(!decodeForwardRequest(frame.payload, decoded));
        request.operation = ForwardOperation::GET; AKK_CLUSTER_CHECK(encodeForwardRequest(request).empty());
        request.operation = ForwardOperation::QUERY_REQUEST; AKK_CLUSTER_CHECK(encodeForwardRequest(request).empty());
        request.operation = ForwardOperation::READ_QUERY;
        AKK_CLUSTER_CHECK(decodeFrame(encodeForwardRequest(request), frame) && decodeForwardRequest(frame.payload, decoded));
        AKK_CLUSTER_CHECK(decoded.operation == ForwardOperation::READ_QUERY);
        request.operation = static_cast<ForwardOperation>(255); AKK_CLUSTER_CHECK(encodeForwardRequest(request).empty());
        request.operation = ForwardOperation::PUT_REQUEST;
        request.entries[0].value.resize(MAX_FORWARD_PAYLOAD); AKK_CLUSTER_CHECK(encodeForwardRequest(request).empty());
        ForwardResponse response;
        response.requestId = 42; response.errorCode = ClusterRoutingCode::NOT_OWNER;
        response.target = {2, "host\"\\\n", 1, 2, 3, 4, 5, 6};
        response.message = "redirect";
        ForwardResponse decodedResponse;
        AKK_CLUSTER_CHECK(decodeFrame(encodeForwardResponse(response), frame) && decodeForwardResponse(frame.payload, decodedResponse));
        AKK_CLUSTER_CHECK(decodedResponse.target.host == response.target.host && decodedResponse.target.raftTerm == 6);
        frame.payload[8] = 2; AKK_CLUSTER_CHECK(!decodeForwardResponse(frame.payload, decodedResponse));
        const auto json = routingErrorJson(ClusterRoutingError(response.errorCode, response.target));
        AKK_CLUSTER_CHECK(json.find("host\\\"\\\\\\u000a") != std::string::npos);
        const ClusterConfig config{{node(1, 24401, 24411), node(2, 24402, 24412)}, ReplicationMode::PARTITIONED,
            {}, ConsistencyOptions{.mode = ConsistencyMode::ASYNC}};
        ClusterRuntimeOptions options;
        options.transportMode = TransportMode::PLAIN;
        options.clusterGroupId = 24400;
        const auto invalid = [&](const ClusterRuntimeOptions& candidate) {
            bool rejected = false;
            try { config.validateRuntime(1, candidate); } catch (const std::invalid_argument&) { rejected = true; }
            AKK_CLUSTER_CHECK(rejected);
        };
        config.validateRuntime(1, options);
        options.routingMode = static_cast<ClusterRoutingMode>(255); invalid(options);
        options.routingMode = ClusterRoutingMode::REDIRECT;
        options.forwardingTimeoutMs = 0; invalid(options);
        options.forwardingTimeoutMs = 300001; invalid(options);
        options.forwardingTimeoutMs = 5000;
        options.queryMaxSpoolBytes = 0; invalid(options);
        options.queryMaxSpoolBytes = UINT64_MAX; invalid(options);
        options.queryMaxSpoolBytes = 1024ull * 1024 * 1024;
        options.queryMaxOpenCursors = 0; invalid(options);
        options.queryMaxOpenCursors = 1025; invalid(options);
        options.queryMaxOpenCursors = 64;
        options.queryCursorIdleTimeoutMs = 0; invalid(options);
        options.queryCursorIdleTimeoutMs = 300001; invalid(options);
        options.queryCursorIdleTimeoutMs = 60000;
        options.apiEndpoints = {{3, 1, 2, 3}}; invalid(options);
        options.apiEndpoints = {{1, 1, 2, 3}, {1, 4, 5, 6}}; invalid(options);
        namespace transfer = akkaradb::engine::cluster::detail;
        auto budget = std::make_shared<transfer::TransferBudget>(ReplicationTransferOptions{});
        const std::vector<uint8_t> oversizedPayload(MAX_FORWARD_PAYLOAD + 1);
        for (const auto type : {ReplMsgType::FORWARD_REQUEST, ReplMsgType::FORWARD_RESPONSE}) {
            bool rejected = false;
            const std::array parts{std::span<const uint8_t>{oversizedPayload}};
            try { (void)transfer::makeMessage(type, parts, budget); } catch (const std::length_error&) { rejected = true; }
            AKK_CLUSTER_CHECK(rejected);
            std::vector<uint8_t> begin(46);
            begin[0] = static_cast<uint8_t>(type);
            for (size_t i = 0; i < 8; ++i) { begin[2 + i] = static_cast<uint8_t>(uint64_t{MAX_FORWARD_PAYLOAD + 1} >> (i * 8)); }
            size_t reads = 0;
            AKK_CLUSTER_CHECK(!transfer::receiveMessage(budget, [&](DecodedFrame& out) {
                ++reads;
                return reads == 1 && decodeFrame(encodeFrame(ReplMsgType::TRANSFER_BEGIN, begin), out);
            }));
            AKK_CLUSTER_CHECK(reads == 1 && budget->used(transfer::TransferBudget::Resource::SPOOL) == 0);
        }
        request.entries[0].value.resize(128u * 1024u);
        const auto message = transfer::messageFromWire(encodeForwardRequest(request), budget);
        std::vector<std::vector<uint8_t>> chunks;
        AKK_CLUSTER_CHECK(transfer::sendMessage(*message, budget, [&](auto wire) {
            chunks.emplace_back(wire.begin(), wire.end()); return true;
        }));
        chunks.insert(chunks.begin() + 2, encodeAck(ReplAck{.seq = 9, .stage = AckStage::DURABLE}));
        size_t next = 0, acknowledgements = 0;
        const auto assembled = transfer::receiveMessage(budget, {}, [&](DecodedFrame& out) {
            return next < chunks.size() && decodeFrame(chunks[next++], out);
        }, {}, [&](const DecodedFrame& out) {
            ReplAck ack;
            const bool valid = decodeAck(out.payload, ack) && ack.seq == 9 && ack.stage == AckStage::DURABLE;
            if (valid) { ++acknowledgements; }
            return valid;
        });
        AKK_CLUSTER_CHECK(assembled && acknowledgements == 1 && decodeForwardRequest(assembled->payload, decoded) &&
            decoded.entries.front().value == request.entries.front().value);
        auto session = std::make_shared<transfer::TransferSession>();
        const auto started = std::chrono::steady_clock::now();
        {
            transfer::ForwardDeadline deadline{started + std::chrono::milliseconds{50}, [session] { session->cancel(); }};
            AKK_CLUSTER_CHECK(!transfer::sendMessage(*message, budget, session, [](auto) { return true; }));
        }
        AKK_CLUSTER_CHECK(std::chrono::steady_clock::now() - started < std::chrono::seconds{1});
    }

    void testNativePartitionRouting(bool secure) {
        using Engine = akkaradb::engine::AkkEngine;
        const auto dir = makeTempDir(secure ? "routing-partition-secure" : "routing-partition");
        const ClusterConfig cfg{{node(1, 24401, 24411), node(2, 24402, 24412)}, ReplicationMode::PARTITIONED,
            {}, ConsistencyOptions{.mode = ConsistencyMode::ASYNC, .ackTimeoutMs = 1000}};
        std::vector<ClusterPeerPublicKeyPin> pins;
        if (secure) {
            for (uint64_t id = 1; id <= 2; ++id) {
                const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
                pins.push_back({id, identity.publicKey});
            }
        }
        const auto makeOptions = [&](uint64_t id, ClusterRoutingMode mode) {
            akkaradb::engine::AkkEngineOptions options;
            options.paths.dataDir = dir / ("n" + std::to_string(id));
            options.components.clusterEnabled = true; options.components.blobEnabled = false;
            options.cluster.config = cfg;
            auto& runtime = options.cluster.runtime;
            runtime.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
            runtime.routingMode = mode; runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
            runtime.clusterGroupId = 24400;
            runtime.apiEndpoints = {{1, 24401, 24501, 24601}, {2, 24402, 24502, 24602}};
            runtime.secure.pinnedPeers = pins; runtime.secure.identitySeedPath = dir / ("identity-" + std::to_string(id));
            writeNodeIdFile(options.paths.dataDir / "node.id", id);
            return options;
        };
        auto n1 = Engine::open(makeOptions(1, ClusterRoutingMode::REDIRECT));
        auto n2 = Engine::open(makeOptions(2, ClusterRoutingMode::FORWARD));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto s1 = n1->stats(), s2 = n2->stats();
            return !s1.cluster.peers.empty() && !s2.cluster.peers.empty() && s1.cluster.peers[0].connected && s2.cluster.peers[0].connected;
        }, std::chrono::seconds{15}));
        const auto k1 = keyOwnedBy(cfg, 1, "route-one"), k2 = keyOwnedBy(cfg, 2, "route-two");
        bool redirected = false;
        try { n1->put(bytesOf(k2), bytesOf("denied")); }
        catch (const ClusterRoutingError& error) {
            redirected = error.code == ClusterRoutingCode::NOT_OWNER && error.target.nodeId == 2 &&
                error.target.httpPort == 24502 && error.target.grpcPort == 24602 && error.target.configurationEpoch == 1;
        }
        AKK_CLUSTER_CHECK(redirected && !n2->get(bytesOf(k2)).has_value());
        n2->putHinted(bytesOf(k1), bytesOf("forwarded"), 0, 0);
        AKK_CLUSTER_CHECK(textOf(*n1->get(bytesOf(k1))) == "forwarded");
        AKK_CLUSTER_CHECK(textOf(*n2->get(bytesOf(k1))) == "forwarded" && n2->exists(bytesOf(k1)));
        const auto k1b = keyOwnedBy(cfg, 1, "route-one-b");
        const std::string batchText = "batch", wrongText = "wrong";
        const std::array<akkaradb::engine::detail::BulkPutEntry, 2> batch{{{bytesOf(k1), bytesOf(batchText)}, {bytesOf(k1b), bytesOf(batchText)}}};
        akkaradb::engine::detail::ProtocolBulkWriter::put(*n2, batch);
        const auto batchValue = n1->get(bytesOf(k1b));
        AKK_CLUSTER_CHECK(batchValue && textOf(*batchValue) == "batch");
        const std::array<akkaradb::engine::detail::BulkPutEntry, 2> mixed{{{bytesOf(k1), bytesOf(wrongText)}, {bytesOf(k2), bytesOf(wrongText)}}};
        bool rejected = false;
        try { akkaradb::engine::detail::ProtocolBulkWriter::put(*n2, mixed); } catch (const ClusterRoutingError& error) { rejected = error.code == ClusterRoutingCode::CROSS_OWNER_BATCH; }
        AKK_CLUSTER_CHECK(rejected && textOf(*n1->get(bytesOf(k1))) == "batch" && !n2->get(bytesOf(k2)).has_value());
        std::vector<std::future<void>> writers;
        for (unsigned i = 0; i < 8; ++i) {
            writers.push_back(std::async(std::launch::async, [&, i] {
                const auto key = keyOwnedBy(cfg, 1, "concurrent-" + std::to_string(i));
                n2->put(bytesOf(key), bytesOf("concurrent"));
                AKK_CLUSTER_CHECK(textOf(*n2->get(bytesOf(key))) == "concurrent");
            }));
        }
        for (auto& writer : writers) { writer.get(); }
        const std::vector<uint8_t> batchItemValue(32u * 1024u, 0x5A);
        std::vector<std::string> largeBatchKeys;
        std::vector<akkaradb::engine::detail::BulkPutEntry> largeBatch;
        largeBatchKeys.reserve(32); largeBatch.reserve(32);
        for (unsigned i = 0; i < 32; ++i) {
            largeBatchKeys.push_back(keyOwnedBy(cfg, 1, "large-forward-" + std::to_string(i)));
            largeBatch.push_back({bytesOf(largeBatchKeys.back()), batchItemValue});
        }
        std::fprintf(stderr, "[cluster] routing large same-owner batch\n");
        akkaradb::engine::detail::ProtocolBulkWriter::put(*n2, largeBatch);
        std::fprintf(stderr, "[cluster] routing large batch readback\n");
        AKK_CLUSTER_CHECK(n2->get(bytesOf(largeBatchKeys.back())) == std::optional{batchItemValue});
        n2->removeHinted(bytesOf(k1), 0, 0); AKK_CLUSTER_CHECK(!n1->exists(bytesOf(k1)));
        const std::vector<uint8_t> oversized(MAX_FORWARD_PAYLOAD, 1);
        rejected = false;
        try { n2->put(bytesOf(k1), oversized); } catch (const ClusterRoutingError& error) { rejected = error.code == ClusterRoutingCode::PAYLOAD_TOO_LARGE; }
        AKK_CLUSTER_CHECK(rejected && !n1->exists(bytesOf(k1)));
        const std::array<akkaradb::engine::detail::BulkPutEntry, 1> oversizedBatch{{{bytesOf(k1), oversized}}};
        rejected = false;
        try { akkaradb::engine::detail::ProtocolBulkWriter::put(*n2, oversizedBatch); } catch (const ClusterRoutingError& error) { rejected = error.code == ClusterRoutingCode::PAYLOAD_TOO_LARGE; }
        AKK_CLUSTER_CHECK(rejected && !n1->exists(bytesOf(k1)));
        std::vector<akkaradb::engine::detail::BulkPutEntry> tooManyEntries(65537, {bytesOf(k1), {}});
        rejected = false;
        try { akkaradb::engine::detail::ProtocolBulkWriter::put(*n2, tooManyEntries); } catch (const ClusterRoutingError& error) { rejected = error.code == ClusterRoutingCode::PAYLOAD_TOO_LARGE; }
        AKK_CLUSTER_CHECK(rejected && !n1->exists(bytesOf(k1)));
        n2->close();
        n2 = Engine::open(makeOptions(2, ClusterRoutingMode::LOCAL_ONLY));
        rejected = false;
        try { n2->put(bytesOf(k1), bytesOf("local-only")); } catch (const ClusterRoutingError& error) { rejected = error.code == ClusterRoutingCode::LOCAL_ONLY && error.target.nodeId == 0; }
        AKK_CLUSTER_CHECK(rejected && !n1->exists(bytesOf(k1)));
        n2->close(); n1->close();
    }

    void testNativeRaftRouting(bool secure) {
        using Engine = akkaradb::engine::AkkEngine;
        const auto dir = makeTempDir(secure ? "routing-raft-secure" : "routing-raft");
        auto raft = onlineRaftMembership(); raft.membership.allowLearners = true;
        const ClusterConfig cfg{{node(1, 24701, 24711), node(2, 24702, 24712), node(3, 24703, 24713, DATA_AND_COORDINATOR | RAFT_LEARNER)},
            ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 2000}, raft};
        std::vector<ClusterPeerPublicKeyPin> pins;
        if (secure) {
            for (uint64_t id = 1; id <= 3; ++id) {
                const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
                pins.push_back({id, identity.publicKey});
            }
        }
        const auto makeOptions = [&](uint64_t id) {
            akkaradb::engine::AkkEngineOptions options;
            options.paths.dataDir = dir / ("n" + std::to_string(id));
            options.components.clusterEnabled = true; options.components.blobEnabled = false;
            options.cluster.config = cfg;
            options.cluster.runtime.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
            options.cluster.runtime.secure.pinnedPeers = pins;
            options.cluster.runtime.secure.identitySeedPath = dir / ("identity-" + std::to_string(id));
            options.cluster.runtime.routingMode = id == 1 ? ClusterRoutingMode::REDIRECT : ClusterRoutingMode::FORWARD;
            options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
            options.cluster.runtime.requests.enabled = true;
            writeNodeIdFile(options.paths.dataDir / "node.id", id);
            return options;
        };
        auto n1 = Engine::open(makeOptions(1)), n2 = Engine::open(makeOptions(2)), n3 = Engine::open(makeOptions(3));
        Engine* nodes[]{n1.get(), n2.get(), n3.get()};
        AKK_CLUSTER_CHECK(waitUntil([&] { return findEngineLeader(nodes) != nullptr; }, std::chrono::seconds{20}));
        auto* leader = findEngineLeader(nodes);
        if (leader != n2.get()) { leader->transferClusterLeadership(2); }
        AKK_CLUSTER_CHECK(waitUntil([&] {
            return findEngineLeader(nodes) == n2.get() && n1->stats().cluster.leaderNodeId == 2 && n3->stats().cluster.leaderNodeId == 2;
        }, std::chrono::seconds{15}));
        bool redirected = false;
        try { n1->put(bytesOf("redirect"), bytesOf("denied")); }
        catch (const ClusterRoutingError& error) { redirected = error.code == ClusterRoutingCode::NOT_OWNER && error.target.nodeId == 2 && error.target.raftTerm > 0; }
        AKK_CLUSTER_CHECK(redirected);
        const auto id = n3->newRequestId();
        const auto first = n3->putWithRequest(id, bytesOf("once-forwarded"), bytesOf("value"));
        const auto second = n3->putWithRequest(id, bytesOf("once-forwarded"), bytesOf("value"));
        AKK_CLUSTER_CHECK(first.status == ClusterRequestStatus::APPLIED && first.sequence == second.sequence && first.logIndex == second.logIndex);
        AKK_CLUSTER_CHECK(n3->queryRequest(id).sequence == first.sequence && n2->stats().putsTotal == 1);
        AKK_CLUSTER_CHECK(textOf(*n3->get(bytesOf("once-forwarded"))) == "value");
        n3->remove(bytesOf("once-forwarded")); AKK_CLUSTER_CHECK(!n3->exists(bytesOf("once-forwarded")));
        const std::vector<uint8_t> batchValue(32u * 1024u, 0xA5);
        std::vector<std::string> batchKeys;
        std::vector<akkaradb::engine::detail::BulkPutEntry> batch;
        batchKeys.reserve(32); batch.reserve(32);
        for (unsigned i = 0; i < 32; ++i) {
            batchKeys.push_back("raft-forward-batch-" + std::to_string(i));
            batch.push_back({bytesOf(batchKeys.back()), batchValue});
        }
        akkaradb::engine::detail::ProtocolBulkWriter::put(*n3, batch);
        AKK_CLUSTER_CHECK(n3->get(bytesOf(batchKeys.back())) == std::optional{batchValue});
        n3->close(); n2->close(); n1->close();
    }

    void testNativeMirrorAckForwarding() {
        using Engine = akkaradb::engine::AkkEngine;
        const auto dir = makeTempDir("routing-mirror-ack");
        const ClusterConfig cfg{{node(1, 24301, 24311), node(2, 24302, 24312)}, ReplicationMode::MIRROR,
            AckPolicy{.stage = AckStage::DURABLE}, ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK,
                .writeConsistency = WriteConsistency::ONE_REPLICA, .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE, .ackTimeoutMs = 1000}, {}, {}, 1};
        const auto makeOptions = [&](uint64_t id) {
            akkaradb::engine::AkkEngineOptions options;
            options.paths.dataDir = dir / ("n" + std::to_string(id));
            options.components.clusterEnabled = true; options.components.blobEnabled = false;
            options.cluster.config = cfg;
            auto& runtime = options.cluster.runtime;
            runtime.transportMode = TransportMode::PLAIN; runtime.clusterGroupId = 24300;
            runtime.startupRole = id == 1 ? NodeStartupRole::PRIMARY : NodeStartupRole::REPLICA;
            runtime.routingMode = ClusterRoutingMode::FORWARD; runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
            writeNodeIdFile(options.paths.dataDir / "node.id", id); return options;
        };
        auto primary = Engine::open(makeOptions(1)), replica = Engine::open(makeOptions(2));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto stats = primary->stats(), replicaStats = replica->stats();
            return !stats.cluster.peers.empty() && stats.cluster.peers[0].connected &&
                !replicaStats.cluster.peers.empty() && replicaStats.cluster.peers[0].connected;
        }));
        replica->put(bytesOf("through-replica"), bytesOf("durable"));
        AKK_CLUSTER_CHECK(textOf(*primary->get(bytesOf("through-replica"))) == "durable");
        AKK_CLUSTER_CHECK(textOf(*replica->get(bytesOf("through-replica"))) == "durable");
        replica->remove(bytesOf("through-replica")); AKK_CLUSTER_CHECK(!primary->exists(bytesOf("through-replica")));
        replica->close(); primary->close();
    }

    void testForwardTimeoutNeverReplays() {
        std::atomic<unsigned> executions{0};
        ClusterRuntimeOptions runtime;
        runtime.transportMode = TransportMode::PLAIN; runtime.clusterGroupId = 24800; runtime.clusterGroupEpoch = 1;
        auto server = ReplicationServer::create(24811, 1, [] { return uint64_t{0}; }, {}, {}, 1, {2},
            runtime);
        server->setForwardCallback([&](const ForwardRequest& request) {
            ++executions; std::this_thread::sleep_for(std::chrono::milliseconds{150});
            ForwardResponse response; response.success = true; response.value = request.entries.front().value; return response;
        });
        server->start();
        auto client = ReplicationClient::create("127.0.0.1", 24811, 2, [] { return uint64_t{0}; }, {},
            runtime, true);
        client->start(); AKK_CLUSTER_CHECK(waitUntil([&] { return client->connected(); }));
        ForwardRequest request; request.operation = ForwardOperation::PUT; request.timeoutMs = 40; request.entries.push_back({{1}, {2}});
        const auto started = std::chrono::steady_clock::now();
        bool unknown = false;
        try { (void)client->forward(request); } catch (const ClusterRoutingError& error) { unknown = error.outcomeUnknown(); }
        AKK_CLUSTER_CHECK(unknown && std::chrono::steady_clock::now() - started < std::chrono::seconds{1});
        AKK_CLUSTER_CHECK(waitUntil([&] { return executions.load() == 1; }));
        std::this_thread::sleep_for(std::chrono::milliseconds{200}); AKK_CLUSTER_CHECK(executions.load() == 1);
        request.timeoutMs = 1000;
        AKK_CLUSTER_CHECK(client->forward(request).success && executions.load() == 2); // late response cannot satisfy a newer ID
        request.timeoutMs = 5000;
        request.entries.front().value.resize(1024u * 1024u, 0x5A);
        const auto large = client->forward(request);
        AKK_CLUSTER_CHECK(large.success && large.value == request.entries.front().value && executions.load() == 3);
        client->close(); server->close();
    }

    void testRaftForwardAdmissionBudget() {
        const auto dir = makeTempDir("routing-raft-admission");
        const ClusterConfig config{{node(1, 24101, 24111), node(2, 24102, 24112), node(3, 24103, 24113)},
            ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 2000}};
        std::atomic<unsigned> executions{0};
        std::atomic<uint32_t> secondBudget{0};
        const auto execute = [&](const ForwardRequest& request) {
            if (++executions == 1) { std::this_thread::sleep_for(std::chrono::milliseconds{300}); }
            else { secondBudget.store(request.timeoutMs); }
            ForwardResponse response; response.success = true; return response;
        };
        auto n1 = makeRuntime(dir / "n1", config, 1, {}, false, execute);
        ClusterRuntimeOptions serialAdmission;
        serialAdmission.raftMaxForwardRequests = 1;
        auto n2 = makeRuntime(dir / "n2", config, 2, serialAdmission, false, execute);
        auto n3 = makeRuntime(dir / "n3", config, 3, {}, false, execute);
        n1->runtime->start(); n2->runtime->start(); n3->runtime->start();
        std::array<RuntimeHarness*, 3> nodes{n1.get(), n2.get(), n3.get()};
        ensurePreferredLeader(nodes, std::chrono::seconds{20});
        ForwardRequest request; request.operation = ForwardOperation::GET; request.timeoutMs = 2000; request.entries.push_back({{1}, {}});
        auto first = std::async(std::launch::async, [&, request] { return n2->runtime->forwardTo(1, request); });
        AKK_CLUSTER_CHECK(waitUntil([&] { return executions.load() == 1; }));
        request.timeoutMs = 1000;
        AKK_CLUSTER_CHECK(n2->runtime->forwardTo(1, request).success && first.get().success);
        AKK_CLUSTER_CHECK(secondBudget.load() > 0 && secondBudget.load() < 900);
        n3->runtime->close(); n2->runtime->close(); n1->runtime->close();
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
            options.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
            writeNodeIdFile(options.paths.dataDir / "node.id", nodeId);
            return options;
        };

        auto n1 = akkaradb::engine::AkkEngine::open(makeOptions(1));
        auto n2 = akkaradb::engine::AkkEngine::open(makeOptions(2));

        const std::string owner1Key = keyOwnedBy(cfg, 1, "engine-owner1");
        const std::string value = "linearizable-owner-value";

        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto stats = n2->stats(); return !stats.cluster.peers.empty() && stats.cluster.peers[0].connected;
        }));
        n2->put(bytesOf(owner1Key), bytesOf("forwarded-owner"));
        AKK_CLUSTER_CHECK(textOf(*n1->get(bytesOf(owner1Key))) == "forwarded-owner");

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
                try {
                    const auto stored = engineGet(*n1, owner2Key);
                    return stored && *stored == std::vector<uint8_t>{owner2Value.begin(), owner2Value.end()};
                }
                catch (const std::runtime_error&) {
                    return false;
                }
            },
            std::chrono::milliseconds{8000}
        ));

        const std::string updatedValue = "owner1-after-interleaved-remote-write";
        n1->put(bytesOf(owner1Key), bytesOf(updatedValue));
        AKK_CLUSTER_CHECK(waitUntil(
            [&] {
                try {
                    const auto stored = engineGet(*n2, owner1Key);
                    return stored && *stored == std::vector<uint8_t>{updatedValue.begin(), updatedValue.end()};
                }
                catch (const std::runtime_error&) {
                    return false;
                }
            },
            std::chrono::milliseconds{8000}
        ));

        n2->close();
        n1->close();
    }

    void testPartitionedDistributedRollback() {
        namespace engine = akkaradb::engine;
        const auto dir = makeTempDir("partitioned-distributed-rollback");
        const ClusterConfig cfg{
            {
                node(1, 21631, 21731),
                node(2, 21632, 21732),
            },
            ReplicationMode::PARTITIONED,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 1000},
            {},
            {},
            0,
            testClusterId(9003),
        };
        auto makeOptions = [&](uint64_t nodeId) {
            engine::AkkEngineOptions options;
            options.paths.dataDir = dir / ("n" + std::to_string(nodeId));
            options.components.clusterEnabled = true;
            options.components.blobEnabled = false;
            options.components.versionLogEnabled = true;
            options.cluster.config = cfg;
            options.cluster.runtime.transportMode = TransportMode::PLAIN;
            options.cluster.runtime.clusterGroupId = 9003;
            options.cluster.runtime.clusterGroupEpoch = 1;
            options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
            options.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
            writeNodeIdFile(options.paths.dataDir / "node.id", nodeId);
            return options;
        };

        auto n1 = engine::AkkEngine::open(makeOptions(1));
        auto n2 = engine::AkkEngine::open(makeOptions(2));
        const std::string key1 = keyOwnedBy(cfg, 1, "rollback-owner1");
        const std::string key2 = keyOwnedBy(cfg, 2, "rollback-owner2");
        n1->put(bytesOf(key1), bytesOf("old-1"));
        n2->put(bytesOf(key2), bytesOf("old-2"));
        std::vector<std::string> pagedKeys;
        pagedKeys.reserve(257);
        for (size_t i = 0; i < 257; ++i) {
            pagedKeys.push_back(keyOwnedBy(cfg, 1, "rollback-page-" + std::to_string(i)));
            n1->put(bytesOf(pagedKeys.back()), bytesOf("old-page"));
        }
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { return n1->createClusterCheckpoint().watermarks.size() == 2; }
            catch (...) { return false; }
        }));
        const auto checkpoint = n1->createClusterCheckpoint();
        n1->put(bytesOf(key1), bytesOf("new-1"));
        n2->put(bytesOf(key2), bytesOf("new-2"));
        for (const auto& key : pagedKeys) { n1->put(bytesOf(key), bytesOf("new-page")); }

        const auto rollback = n1->rollbackTo(checkpoint);
        AKK_CLUSTER_CHECK(rollback.complete());
        AKK_CLUSTER_CHECK(rollback.appliedCount() == 2 + pagedKeys.size());
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto value1 = engineGet(*n1, key1);
            const auto value2 = engineGet(*n2, key2);
            if (!value1 || !value2 || textOf(*value1) != "old-1" || textOf(*value2) != "old-2") { return false; }
            return std::ranges::all_of(pagedKeys, [&](const std::string& key) {
                const auto value = engineGet(*n1, key);
                return value && textOf(*value) == "old-page";
            });
        }, std::chrono::milliseconds{8000}));

        bool scalarRejected = false;
        try { (void)n1->rollbackKey(bytesOf(key1), uint64_t{1}); }
        catch (const std::runtime_error&) { scalarRejected = true; }
        AKK_CLUSTER_CHECK(scalarRejected);

        const auto stream1 = *std::ranges::find(checkpoint.watermarks, uint64_t{1}, &engine::Revision::streamId);
        const auto stream2 = *std::ranges::find(checkpoint.watermarks, uint64_t{2}, &engine::Revision::streamId);
        n1->put(bytesOf(key1), bytesOf("deferred-new-1"));
        const auto deferredApply = n1->rollbackKey(
            bytesOf(key1), stream1,
            engine::RollbackOptions{.execution = engine::RollbackExecutionMode::NEXT_STARTUP}
        );
        AKK_CLUSTER_CHECK(deferredApply.deferredCount() == 1);
        n2->put(bytesOf(key2), bytesOf("deferred-new-2"));
        const auto deferredConflict = n1->rollbackKey(
            bytesOf(key2), stream2,
            engine::RollbackOptions{.execution = engine::RollbackExecutionMode::NEXT_STARTUP}
        );
        AKK_CLUSTER_CHECK(deferredConflict.deferredCount() == 1);
        n2->put(bytesOf(key2), bytesOf("after-schedule-2"));
        n1->close();
        n2->close();

        n1 = engine::AkkEngine::open(makeOptions(1));
        n2 = engine::AkkEngine::open(makeOptions(2));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try {
                const auto value1 = engineGet(*n1, key1);
                const auto value2 = engineGet(*n2, key2);
                return value1 && value2 && textOf(*value1) == "old-1" && textOf(*value2) == "after-schedule-2";
            }
            catch (...) { return false; }
        }, std::chrono::milliseconds{10000}));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            std::error_code error;
            return std::filesystem::file_size(dir / "n1" / "rollback-journal.akrb", error) == 13 && !error;
        }, std::chrono::milliseconds{10000}));
        n1->close();
        n2->close();
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
            options.cluster.runtime.routingMode = nodeId == 2 ? ClusterRoutingMode::REDIRECT : ClusterRoutingMode::FORWARD;
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

        const std::string forwardedValue = "forwarded-stripe";
        n3->put(bytesOf(owner1Key), bytesOf(forwardedValue));
        const auto forwardedOwnerValue = engineGet(*n1, owner1Key);
        AKK_CLUSTER_CHECK(forwardedOwnerValue && textOf(*forwardedOwnerValue) == forwardedValue);
        const auto batchKey = keyOwnedBy(cfg, 1, "stripe-forward-batch");
        const std::array<akkaradb::engine::detail::BulkPutEntry, 2> batch{{
            {bytesOf(owner1Key), bytesOf(value)}, {bytesOf(batchKey), bytesOf(value)}}};
        akkaradb::engine::detail::ProtocolBulkWriter::put(*n3, batch);
        AKK_CLUSTER_CHECK(textOf(*n1->get(bytesOf(batchKey))) == value);
        n3->remove(bytesOf(owner1Key));
        AKK_CLUSTER_CHECK(!n1->exists(bytesOf(owner1Key)));

        n3->close();
        n2->close();
        n1->close();
    }

    void testStripeEngineDegradedWriteAndAutomaticRebuild() {
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
            options.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
            options.cluster.runtime.stripeWriteCommitMode = StripeWriteCommitMode::DATA_SHARDS;
            options.cluster.runtime.stripeReadCoordinatorMode = coordinatorMode;
            options.cluster.runtime.stripeRebuildIntervalMs = 100;
            options.cluster.runtime.stripeRebuildBatchKeys = 16;
            writeNodeIdFile(options.paths.dataDir / "node.id", nodeId);
            return options;
        };

        auto invalidOptions = makeOptions(1, StripeReadCoordinatorMode::OWNER);
        invalidOptions.cluster.runtime.stripeWriteCommitMode = static_cast<StripeWriteCommitMode>(2);
        bool rejectedInvalidMode = false;
        try {
            auto engine = akkaradb::engine::AkkEngine::open(invalidOptions);
            (void)engine;
        }
        catch (const std::invalid_argument&) { rejectedInvalidMode = true; }
        AKK_CLUSTER_CHECK(rejectedInvalidMode);

        auto n1 = akkaradb::engine::AkkEngine::open(makeOptions(1, StripeReadCoordinatorMode::LOCAL_COORDINATOR));
        auto n2 = akkaradb::engine::AkkEngine::open(makeOptions(2, StripeReadCoordinatorMode::LOCAL_COORDINATOR));
        auto n3 = akkaradb::engine::AkkEngine::open(makeOptions(3, StripeReadCoordinatorMode::LOCAL_COORDINATOR));
        const auto key = keyOwnedBy(cfg, 1, "degraded-write-rebuild");
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { n1->put(bytesOf(key), bytesOf("fully-redundant")); return true; }
            catch (const std::runtime_error&) { return false; }
        }));
        AKK_CLUSTER_CHECK(hasStripeMetadataMagic(*n1, "AKSM1"));

        n3->close();
        n3.reset();
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto stats = n1->stats();
            const auto peer = std::ranges::find(stats.cluster.peers, uint64_t{3}, &akkaradb::engine::EngineStats::ClusterStats::PeerStats::nodeId);
            return peer != stats.cluster.peers.end() && !peer->connected;
        }));
        n1->put(bytesOf(key), bytesOf("committed-degraded"));
        AKK_CLUSTER_CHECK(n1->stats().cluster.health == akkaradb::engine::ClusterHealthState::REBUILDING);

        n3 = akkaradb::engine::AkkEngine::open(makeOptions(3, StripeReadCoordinatorMode::LOCAL_COORDINATOR));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto n1Stats = n1->stats();
            const auto n2Stats = n2->stats();
            const auto n3Stats = n3->stats();
            return n1Stats.stripeRebuild.succeeded + n2Stats.stripeRebuild.succeeded +
                    n3Stats.stripeRebuild.succeeded >= 1 &&
                stripeInternalCounts(*n3).first == 1;
        }, std::chrono::milliseconds{15000}));
        const auto rebuilt = engineGet(*n2, key);
        AKK_CLUSTER_CHECK(rebuilt && textOf(*rebuilt) == "committed-degraded");

        n2->close();
        n2.reset();
        n3->close();
        n3.reset();
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto stats = n1->stats();
            return std::ranges::count_if(stats.cluster.peers, [](const auto& peer) { return !peer.connected; }) == 2;
        }));
        bool rejectedBelowDataShards = false;
        try { n1->put(bytesOf(key), bytesOf("must-not-publish")); }
        catch (const std::runtime_error&) { rejectedBelowDataShards = true; }
        AKK_CLUSTER_CHECK(rejectedBelowDataShards);

        n2 = akkaradb::engine::AkkEngine::open(makeOptions(2, StripeReadCoordinatorMode::LOCAL_COORDINATOR));
        n3 = akkaradb::engine::AkkEngine::open(makeOptions(3, StripeReadCoordinatorMode::LOCAL_COORDINATOR));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try {
                const auto value = engineGet(*n2, key);
                return value && textOf(*value) == "committed-degraded";
            }
            catch (const std::runtime_error&) { return false; }
        }, std::chrono::milliseconds{15000}));
        n3->close();
        n2->close();
        n1->close();
    }

    void testStripeMetadataLeaderAutomaticRebuild() {
        const auto dir = makeTempDir("stripe-metadata-leader-rebuild");
        constexpr uint32_t failoverCapabilities = DATA_AND_COORDINATOR |
            static_cast<uint32_t>(NodeCapability::STRIPE_FAILOVER_ELIGIBLE);
        const ClusterConfig cfg{
            {
                node(1, 21634, 21734),
                node(2, 21635, 21735),
                node(3, 21636, 21736, failoverCapabilities),
            },
            ReplicationMode::STRIPE,
            AckPolicy{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::APPLIED},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 250},
            {},
            StripeOptions{.dataShards = 1, .parityShards = 1},
        };

        ClusterRouter router{cfg};
        std::string key;
        for (uint32_t i = 0; i < 10000; ++i) {
            auto candidate = std::string{"failover-rebuild-"} + std::to_string(i);
            const auto targets = router.writeTargets(bytesOf(candidate));
            if (targets.size() == 2 && targets[0].nodeId == 1 && targets[1].nodeId == 2) {
                key = std::move(candidate);
                break;
            }
        }
        AKK_CLUSTER_CHECK(!key.empty());

        auto makeOptions = [&](uint64_t nodeId) {
            akkaradb::engine::AkkEngineOptions options;
            options.paths.dataDir = dir / ("n" + std::to_string(nodeId));
            options.components.clusterEnabled = true;
            options.components.blobEnabled = false;
            options.cluster.config = cfg;
            options.cluster.runtime.transportMode = TransportMode::PLAIN;
            options.cluster.runtime.clusterGroupId = 9013;
            options.cluster.runtime.clusterGroupEpoch = 1;
            options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
            options.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
            options.cluster.runtime.stripeRebuildIntervalMs = 100;
            writeNodeIdFile(options.paths.dataDir / "node.id", nodeId);
            return options;
        };

        auto owner = akkaradb::engine::AkkEngine::open(makeOptions(1));
        auto shard = akkaradb::engine::AkkEngine::open(makeOptions(2));
        auto failover = akkaradb::engine::AkkEngine::open(makeOptions(3));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { owner->put(bytesOf(key), bytesOf("full-generation")); return true; }
            catch (const std::runtime_error&) { return false; }
        }));
        AKK_CLUSTER_CHECK(waitUntil([&] { return hasStripeMetadataMagic(*failover, "AKSM1"); }));

        shard->close();
        shard.reset();
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto stats = owner->stats();
            const auto peer = std::ranges::find(stats.cluster.peers, uint64_t{2}, &akkaradb::engine::EngineStats::ClusterStats::PeerStats::nodeId);
            return peer != stats.cluster.peers.end() && !peer->connected;
        }));
        owner->put(bytesOf(key), bytesOf("sequenced-degraded"));

        shard = akkaradb::engine::AkkEngine::open(makeOptions(2));
        const bool rebuiltByMetadataLeader = waitUntil([&] {
            const auto ownerStats = owner->stats();
            const auto shardStats = shard->stats();
            const auto failoverStats = failover->stats();
            return ownerStats.stripeRebuild.succeeded + shardStats.stripeRebuild.succeeded +
                    failoverStats.stripeRebuild.succeeded >= 1 &&
                stripeInternalCounts(*shard).first >= 1;
        }, std::chrono::milliseconds{15000});
        if (!rebuiltByMetadataLeader) {
            const auto ownerStats = owner->stats();
            const auto shardStats = shard->stats();
            const auto failoverStats = failover->stats();
            std::fprintf(
                stderr,
                "stripe metadata leader rebuild: owner=%llu shard=%llu failover=%llu shardRecords=%zu\n",
                static_cast<unsigned long long>(ownerStats.stripeRebuild.succeeded),
                static_cast<unsigned long long>(shardStats.stripeRebuild.succeeded),
                static_cast<unsigned long long>(failoverStats.stripeRebuild.succeeded),
                stripeInternalCounts(*shard).first
            );
        }
        AKK_CLUSTER_CHECK(rebuiltByMetadataLeader);
        const auto rebuilt = engineGet(*owner, key);
        AKK_CLUSTER_CHECK(rebuilt && textOf(*rebuilt) == "sequenced-degraded");

        failover->close();
        shard->close();
        owner->close();
    }

    void testStripeMetadataRaftLeaderFailover() {
        const auto dir = makeTempDir("stripe-metadata-raft-leader-failover");
        constexpr uint32_t failoverCapabilities = DATA_AND_COORDINATOR |
            static_cast<uint32_t>(NodeCapability::STRIPE_FAILOVER_ELIGIBLE);
        const ClusterConfig cfg{
            {
                node(1, 21637, 21737),
                node(2, 21638, 21738),
                node(3, 21639, 21739, failoverCapabilities),
            },
            ReplicationMode::STRIPE,
            AckPolicy{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::APPLIED},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 250},
            {},
            StripeOptions{.dataShards = 1, .parityShards = 1},
        };
        constexpr std::array<uint64_t, 2> placement{1, 2};
        const auto key = keyPlacedOn(cfg, placement, "metadata-leader-failover");
        const auto makeOptions = [&](uint64_t nodeId) {
            akkaradb::engine::AkkEngineOptions options;
            options.paths.dataDir = dir / ("n" + std::to_string(nodeId));
            options.components.clusterEnabled = true;
            options.components.blobEnabled = false;
            options.cluster.config = cfg;
            options.cluster.runtime.transportMode = TransportMode::PLAIN;
            options.cluster.runtime.clusterGroupId = 9014;
            options.components.versionLogEnabled = true;
            options.cluster.runtime.clusterGroupEpoch = 1;
            options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
            options.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
            options.cluster.runtime.stripeReadCoordinatorMode = StripeReadCoordinatorMode::LOCAL_COORDINATOR;
            options.cluster.runtime.raftSnapshot.minLogEntries = 2;
            options.cluster.runtime.raftSnapshot.minLogBytes = 1024ull * 1024 * 1024;
            options.cluster.runtime.raftSnapshot.maxIntervalMs = 60'000;
            writeNodeIdFile(options.paths.dataDir / "node.id", nodeId);
            return options;
        };

        auto n1 = akkaradb::engine::AkkEngine::open(makeOptions(1));
        auto n2 = akkaradb::engine::AkkEngine::open(makeOptions(2));
        auto n3 = akkaradb::engine::AkkEngine::open(makeOptions(3));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { n1->put(bytesOf(key), bytesOf("before-leader-loss")); return true; }
            catch (const std::runtime_error&) { return false; }
        }));

        uint64_t oldLeader = 0;
        uint64_t oldTerm = 0;
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto a = n1->stats().cluster;
            const auto b = n2->stats().cluster;
            const auto c = n3->stats().cluster;
            if (a.stripeMetadataLeaderNodeId == 0 || a.stripeMetadataLeaderNodeId != b.stripeMetadataLeaderNodeId ||
                a.stripeMetadataLeaderNodeId != c.stripeMetadataLeaderNodeId) {
                return false;
            }
            oldLeader = a.stripeMetadataLeaderNodeId;
            oldTerm = a.stripeMetadataRaftTerm;
            return true;
        }));

        if (oldLeader == 1) { n1->close(); n1.reset(); }
        else if (oldLeader == 2) { n2->close(); n2.reset(); }
        else { n3->close(); n3.reset(); }

        auto survivorStats = [&]() {
            if (n1) { return n1->stats().cluster; }
            if (n2) { return n2->stats().cluster; }
            return n3->stats().cluster;
        };
        auto metadataLeaderStats = [&]() {
            for (auto* engine : {n1.get(), n2.get(), n3.get()}) {
                if (!engine) { continue; }
                const auto stats = engine->stats();
                if (stats.nodeId == stats.cluster.stripeMetadataLeaderNodeId) { return stats.cluster; }
            }
            return survivorStats();
        };
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto stats = survivorStats();
            return stats.stripeMetadataLeaderNodeId != 0 && stats.stripeMetadataLeaderNodeId != oldLeader &&
                stats.stripeMetadataRaftTerm > oldTerm;
        }, std::chrono::milliseconds{10000}));

        auto* writer = n1 ? n1.get() : n3.get();
        AKK_CLUSTER_CHECK(writer != nullptr);
        for (unsigned generation = 0; generation < 6; ++generation) {
            const auto value = "after-leader-loss-" + std::to_string(generation);
            AKK_CLUSTER_CHECK(waitUntil([&] {
                try { writer->put(bytesOf(key), bytesOf(value)); return true; }
                catch (const std::runtime_error&) { return false; }
            }, std::chrono::milliseconds{10000}));
        }
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try {
                const auto value = engineGet(*writer, key);
                return value && textOf(*value) == "after-leader-loss-5";
            }
            catch (const std::runtime_error&) { return false; }
        }));
        // Placement authority retains its complete metadata journal so a new
        // holder can recover history, rather than only a compacted KV head.
        AKK_CLUSTER_CHECK(metadataLeaderStats().stripeMetadataSnapshotIndex == 0);
        const auto expectedHistory = akk_test::collectHistory(writer->history(bytesOf(key)));
        AKK_CLUSTER_CHECK(expectedHistory.size() >= 7 && textOf(expectedHistory.front().value) == "before-leader-loss");

        if (oldLeader == 1) { n1 = akkaradb::engine::AkkEngine::open(makeOptions(1)); }
        else if (oldLeader == 2) { n2 = akkaradb::engine::AkkEngine::open(makeOptions(2)); }
        else { n3 = akkaradb::engine::AkkEngine::open(makeOptions(3)); }
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto restarted = oldLeader == 1 ? n1->stats().cluster : oldLeader == 2 ? n2->stats().cluster : n3->stats().cluster;
            return restarted.stripeMetadataLeaderNodeId != 0 &&
                restarted.stripeMetadataAppliedIndex >= metadataLeaderStats().stripeMetadataCommitIndex;
        }, std::chrono::milliseconds{10000}));
        auto* restarted = oldLeader == 1 ? n1.get() : oldLeader == 2 ? n2.get() : n3.get();
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try {
                const auto value = engineGet(*restarted, key);
                return value && textOf(*value) == "after-leader-loss-5";
            }
            catch (const std::runtime_error&) { return false; }
        }));
        const auto recoveredHistory = akk_test::collectHistory(restarted->history(bytesOf(key)));
        AKK_CLUSTER_CHECK(recoveredHistory.size() == expectedHistory.size());
        for (size_t i = 0; i < expectedHistory.size(); ++i) {
            AKK_CLUSTER_CHECK(recoveredHistory[i].seq == expectedHistory[i].seq && recoveredHistory[i].value == expectedHistory[i].value &&
                recoveredHistory[i].sourceNodeId == expectedHistory[i].sourceNodeId && recoveredHistory[i].timestampNs == expectedHistory[i].timestampNs);
        }

        n3->close();
        n2->close();
        n1->close();
    }

    void testRaid6PresetMetadataQuorumSurvivesTwoFailures() {
        const auto dir = makeTempDir("stripe-metadata-two-failure-quorum");
        const ClusterConfig cfg{
            {
                node(1, 21645, 21745),
                node(2, 21646, 21746),
                node(3, 21647, 21747),
                node(4, 21648, 21748),
                node(5, 21649, 21749),
            },
            RaidOptions{.preset = RaidPreset::RAID6, .dataShards = 2},
            AckPolicy{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::APPLIED},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 250},
            testClusterId(9015),
        };
        AKK_CLUSTER_CHECK(cfg.mode() == ReplicationMode::STRIPE);
        AKK_CLUSTER_CHECK(cfg.raidPreset() == RaidPreset::RAID6);
        AKK_CLUSTER_CHECK(cfg.stripe().dataShards == 2);
        AKK_CLUSTER_CHECK(cfg.stripe().parityShards == 2);
        const auto configPath = dir / "cluster.cfg";
        ClusterConfig::save(configPath, cfg);
        const auto loaded = ClusterConfig::load(configPath);
        AKK_CLUSTER_CHECK(loaded.raidPreset() == RaidPreset::RAID6);
        AKK_CLUSTER_CHECK(loaded.stripe().dataShards == 2);
        AKK_CLUSTER_CHECK(loaded.stripe().parityShards == 2);

        bool rejectedFourNodeRaid6 = false;
        try {
            (void)ClusterConfig{
                {
                    node(1, 21650, 21750),
                    node(2, 21651, 21751),
                    node(3, 21652, 21752),
                    node(4, 21653, 21753),
                },
                RaidOptions{.preset = RaidPreset::RAID6, .dataShards = 2},
                AckPolicy{},
            };
        }
        catch (const std::invalid_argument&) { rejectedFourNodeRaid6 = true; }
        AKK_CLUSTER_CHECK(rejectedFourNodeRaid6);
        constexpr std::array<uint64_t, 4> placement{1, 2, 3, 4};
        const auto key = keyPlacedOn(cfg, placement, "metadata-two-failure-quorum");
        const auto makeOptions = [&](uint64_t nodeId) {
            akkaradb::engine::AkkEngineOptions options;
            options.paths.dataDir = dir / ("n" + std::to_string(nodeId));
            options.components.clusterEnabled = true;
            options.components.blobEnabled = false;
            options.cluster.config = cfg;
            options.cluster.runtime.transportMode = TransportMode::PLAIN;
            options.cluster.runtime.clusterGroupId = 9015;
            options.cluster.runtime.clusterGroupEpoch = 1;
            options.cluster.runtime.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
            options.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
            options.cluster.runtime.stripeReadCoordinatorMode = StripeReadCoordinatorMode::LOCAL_COORDINATOR;
            writeNodeIdFile(options.paths.dataDir / "node.id", nodeId);
            return options;
        };

        auto n1 = akkaradb::engine::AkkEngine::open(makeOptions(1));
        auto n2 = akkaradb::engine::AkkEngine::open(makeOptions(2));
        auto n3 = akkaradb::engine::AkkEngine::open(makeOptions(3));
        auto n4 = akkaradb::engine::AkkEngine::open(makeOptions(4));
        auto n5 = akkaradb::engine::AkkEngine::open(makeOptions(5));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { n1->put(bytesOf(key), bytesOf("all-voters")); return true; }
            catch (const std::runtime_error&) { return false; }
        }, std::chrono::milliseconds{15000}));

        n4->close();
        n4.reset();
        n3->close();
        n3.reset();
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try { n1->put(bytesOf(key), bytesOf("two-voters-down")); return true; }
            catch (const std::runtime_error&) { return false; }
        }, std::chrono::milliseconds{15000}));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try {
                const auto value = engineGet(*n1, key);
                return value && textOf(*value) == "two-voters-down";
            }
            catch (const std::runtime_error&) { return false; }
        }, std::chrono::milliseconds{15000}));
        const auto stats = n1->stats().cluster;
        AKK_CLUSTER_CHECK(stats.stripeMetadataLeaderNodeId != 0);
        AKK_CLUSTER_CHECK(stats.stripeMetadataCommitIndex == stats.stripeMetadataAppliedIndex);

        n5->close();
        n2->close();
        n1->close();
    }

    void testStripeEngineRejectsForeignConfiguration() {
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
            options.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
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
        catch (const std::exception&) {
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
        injectedFailure.trigger();
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
        injectedFailure.trigger();
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

        const erasure::ErasureLayout raid0Layout{.dataShards = 3, .parityShards = 0};
        const auto raid0Shards = erasure::RsErasureCodec::encode(bytesOf(value), raid0Layout);
        AKK_CLUSTER_CHECK(raid0Shards.size() == 3);
        AKK_CLUSTER_CHECK(textOf(erasure::RsErasureCodec::decode(raid0Shards, raid0Layout)) == value);
        bool raid0MissingShardRejected = false;
        try {
            (void)erasure::RsErasureCodec::decode(
                std::vector<erasure::ErasureShard>{raid0Shards[0], raid0Shards[2]}, raid0Layout
            );
        }
        catch (const std::runtime_error&) { raid0MissingShardRejected = true; }
        AKK_CLUSTER_CHECK(raid0MissingShardRejected);
        bool raid0RepairRejected = false;
        try {
            (void)erasure::RsErasureCodec::repairOne(
                1, std::vector<erasure::ErasureShard>{raid0Shards[0], raid0Shards[2]}, raid0Layout
            );
        }
        catch (const std::runtime_error&) { raid0RepairRejected = true; }
        AKK_CLUSTER_CHECK(raid0RepairRejected);

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

    class RaftTestSocket {
    public:
#ifdef _WIN32
        using Handle = SOCKET;
        static constexpr Handle INVALID = INVALID_SOCKET;
#else
        using Handle = int;
        static constexpr Handle INVALID = -1;
#endif
        Handle handle = INVALID;
        RaftTestSocket() {
#ifdef _WIN32
            struct Winsock {
                Winsock() { WSADATA data; AKK_CLUSTER_CHECK(::WSAStartup(MAKEWORD(2, 2), &data) == 0); }
                ~Winsock() { ::WSACleanup(); }
            };
            static Winsock winsock;
#endif
        }
        explicit RaftTestSocket(Handle socket) : RaftTestSocket() { handle = socket; configure(); }
        ~RaftTestSocket() { close(); }
        RaftTestSocket(const RaftTestSocket&) = delete;
        RaftTestSocket& operator=(const RaftTestSocket&) = delete;
        void configure() const {
#if !defined(_WIN32) && defined(SO_NOSIGPIPE)
            const int enabled = 1;
            AKK_CLUSTER_CHECK(::setsockopt(handle, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled)) == 0);
#endif
        }
        void shutdown() const {
            if (handle == INVALID) { return; }
#ifdef _WIN32
            ::shutdown(handle, SD_BOTH);
#else
            ::shutdown(handle, SHUT_RDWR);
#endif
        }
        void close() {
            if (handle == INVALID) { return; }
#ifdef _WIN32
            ::closesocket(handle);
#else
            ::close(handle);
#endif
            handle = INVALID;
        }
        bool connect(const char* host, uint16_t port) {
            handle = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
            if (handle == INVALID) { return false; }
            configure();
            sockaddr_in address{};
            address.sin_family = AF_INET;
            if (::inet_pton(AF_INET, host, &address.sin_addr) != 1) { return false; }
            address.sin_port = htons(port);
            return ::connect(handle, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0;
        }
        void listen(uint16_t port) {
            handle = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
            AKK_CLUSTER_CHECK(handle != INVALID);
#ifndef _WIN32
            const int reuse = 1;
            AKK_CLUSTER_CHECK(::setsockopt(handle, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) == 0);
#endif
            sockaddr_in address{};
            address.sin_family = AF_INET;
            AKK_CLUSTER_CHECK(::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr) == 1);
            address.sin_port = htons(port);
            AKK_CLUSTER_CHECK(::bind(handle, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
            AKK_CLUSTER_CHECK(::listen(handle, 16) == 0);
        }
        bool send(std::span<const uint8_t> bytes) const {
            size_t offset = 0;
            while (offset < bytes.size()) {
#ifdef _WIN32
                const int count = ::send(handle, reinterpret_cast<const char*>(bytes.data() + offset), static_cast<int>(bytes.size() - offset), 0);
#else
#ifdef MSG_NOSIGNAL
                constexpr int flags = MSG_NOSIGNAL;
#else
                constexpr int flags = 0;
#endif
                const auto count = ::send(handle, bytes.data() + offset, bytes.size() - offset, flags);
#endif
                if (count <= 0) { return false; }
                offset += static_cast<size_t>(count);
            }
            return true;
        }
        bool receive(std::span<uint8_t> bytes) const {
            size_t offset = 0;
            while (offset < bytes.size()) {
                const auto count = ::recv(handle, reinterpret_cast<char*>(bytes.data() + offset), static_cast<int>(bytes.size() - offset), 0);
                if (count <= 0) { return false; }
                offset += static_cast<size_t>(count);
            }
            return true;
        }
        DecodedFrame receiveFrame() const {
            std::vector<uint8_t> wire(ReplFrameHeader::SIZE);
            AKK_CLUSTER_CHECK(receive(wire));
            uint32_t size = 0;
            for (size_t i = 0; i < 4; ++i) { size |= uint32_t{wire[6 + i]} << (i * 8); }
            AKK_CLUSTER_CHECK(size <= ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE);
            wire.resize(ReplFrameHeader::SIZE + size);
            AKK_CLUSTER_CHECK(receive(std::span{wire}.subspan(ReplFrameHeader::SIZE)));
            DecodedFrame frame;
            AKK_CLUSTER_CHECK(decodeFrame(wire, frame));
            return frame;
        }
    };

    // Transparent at the byte level, so the same fault exercises PLAIN and SECURE.
    // Only inbound traffic to the selected follower is cut; it can still campaign
    // against the healthy majority, a stronger case than a completely offline peer.
    class RaftInboundFaultProxy {
        struct Connection { RaftTestSocket client, backend; };
        RaftTestSocket listener_;
        std::atomic<bool> stopping_{false}, failed_{false};
        std::atomic<bool> rejectMutations_{false};
        bool inspectFrames_ = false;
        bool blocked_ = true;
        std::mutex mutex_;
        std::vector<std::shared_ptr<Connection>> connections_;
        std::vector<std::thread> workers_;
        std::thread acceptThread_;
        static void pump(const RaftTestSocket& source, const RaftTestSocket& destination) {
            std::array<uint8_t, 16384> buffer{};
            for (;;) {
                const auto count = ::recv(source.handle, reinterpret_cast<char*>(buffer.data()), static_cast<int>(buffer.size()), 0);
                if (count <= 0 || !destination.send(std::span{buffer}.first(static_cast<size_t>(count)))) { break; }
            }
            source.shutdown(); destination.shutdown();
        }
        void pumpRequests(const RaftTestSocket& source, const RaftTestSocket& destination) {
            if (!inspectFrames_) { pump(source, destination); return; }
            const auto number = [](std::span<const uint8_t> bytes, size_t offset, size_t width) {
                uint64_t value = 0;
                for (size_t i = 0; i < width; ++i) { value |= uint64_t{bytes[offset + i]} << (8 * i); }
                return value;
            };
            for (;;) {
                std::vector<uint8_t> wire(ReplFrameHeader::SIZE);
                if (!source.receive(wire)) { break; }
                const auto length = static_cast<size_t>(number(wire, 6, 4));
                if (length > ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE) { failed_.store(true); break; }
                wire.resize(ReplFrameHeader::SIZE + length);
                if (!source.receive(std::span{wire}.subspan(ReplFrameHeader::SIZE))) { break; }
                DecodedFrame frame;
                if (!decodeFrame(wire, frame)) { failed_.store(true); break; }
                bool reject = false;
                if (rejectMutations_.load() && frame.type == ReplMsgType::RAFT_APPEND_ENTRIES && frame.payload.size() >= 44) {
                    size_t cursor = 44;
                    const auto count = number(frame.payload, 40, 4);
                    for (uint64_t i = 0; i < count && cursor + 4 <= frame.payload.size(); ++i) {
                        const auto size = static_cast<size_t>(number(frame.payload, cursor, 4)); cursor += 4;
                        if (size < 25 || size > frame.payload.size() - cursor) { failed_.store(true); break; }
                        if (frame.payload[cursor + 24] == 0) { reject = true; }
                        cursor += size;
                    }
                }
                if (reject) {
                    std::vector<uint8_t> response(frame.payload.begin(), frame.payload.begin() + 8);
                    response.push_back(0);
                    const auto prev = number(frame.payload, 16, 8);
                    for (uint64_t value : {prev, prev + 1, uint64_t{0}}) {
                        for (size_t i = 0; i < 8; ++i) { response.push_back(static_cast<uint8_t>(value >> (8 * i))); }
                    }
                    if (!source.send(encodeFrame(ReplMsgType::RAFT_APPEND_ENTRIES_RESPONSE, response))) { break; }
                }
                else if (!destination.send(wire)) { break; }
            }
            source.shutdown(); destination.shutdown();
        }
    public:
        explicit RaftInboundFaultProxy(uint16_t port, bool inspectFrames = false) : inspectFrames_{inspectFrames} {
            listener_.listen(port);
            acceptThread_ = std::thread([this, port] {
                try {
                    while (!stopping_.load()) {
                        fd_set readable;
                        FD_ZERO(&readable); FD_SET(listener_.handle, &readable);
                        timeval timeout{0, 100000};
#ifdef _WIN32
                        const int ready = ::select(0, &readable, nullptr, nullptr, &timeout);
#else
                        const int ready = ::select(listener_.handle + 1, &readable, nullptr, nullptr, &timeout);
#endif
                        if (ready == 0) { continue; }
                        if (ready < 0) { break; }
                        auto connection = std::make_shared<Connection>();
                        connection->client.handle = ::accept(listener_.handle, nullptr, nullptr);
                        if (connection->client.handle == RaftTestSocket::INVALID) { break; }
                        connection->client.configure();
                        std::lock_guard lock{mutex_};
                        if (blocked_ || stopping_.load() || !connection->backend.connect("127.0.0.2", port)) { continue; }
                        connections_.push_back(connection);
                        workers_.emplace_back([this, connection] {
                            try {
                                std::thread upload([this, connection] { pumpRequests(connection->client, connection->backend); });
                                pump(connection->backend, connection->client);
                                upload.join();
                            }
                            catch (...) { failed_.store(true); connection->client.shutdown(); connection->backend.shutdown(); }
                        });
                    }
                }
                catch (...) { failed_.store(true); }
            });
        }
        ~RaftInboundFaultProxy() {
            stopping_.store(true);
            setBlocked(true);
            listener_.shutdown();
            acceptThread_.join();
            for (auto& worker : workers_) { worker.join(); }
        }
        void setBlocked(bool blocked) {
            std::lock_guard lock{mutex_};
            blocked_ = blocked;
            if (blocked) {
                for (const auto& connection : connections_) { connection->client.shutdown(); connection->backend.shutdown(); }
            }
        }
        bool failed() const { return failed_.load(); }
        void rejectMutations(bool enabled) { rejectMutations_.store(enabled); }
    };

    void appendRaftTestInteger(std::vector<uint8_t>& bytes, uint64_t value, size_t width = 8) {
        for (size_t i = 0; i < width; ++i) { bytes.push_back(static_cast<uint8_t>(value >> (i * 8))); }
    }

    void openRaftTestPeer(RaftTestSocket& socket, uint16_t port, uint64_t sender, const ClusterConfig& config, const char domain = '1') {
        AKK_CLUSTER_CHECK(socket.connect("127.0.0.1", port));
        // Independent wire fixture: changing the protocol must update this contract too.
        const auto consistency = config.consistency();
        const auto membership = config.raft().membership;
        const std::vector<uint8_t> fingerprintInput{'A', 'K', 'R', 'P', static_cast<uint8_t>(domain),
            static_cast<uint8_t>(config.mode()), static_cast<uint8_t>(consistency.mode), static_cast<uint8_t>(config.failover()),
            static_cast<uint8_t>(consistency.writeConsistency), static_cast<uint8_t>(consistency.ackTimeoutAction),
            static_cast<uint8_t>(consistency.replicaLagAction), static_cast<uint8_t>(membership.mode),
            static_cast<uint8_t>(membership.allowOnlineVoterChanges), static_cast<uint8_t>(membership.allowLearners),
            static_cast<uint8_t>(RaftBlobPolicy::REJECT), 1};
        const std::array parts{std::span<const uint8_t>{fingerprintInput}};
        const auto fingerprint = akkaradb::crypto::hash256(parts);
        std::vector<uint8_t> hello;
        appendRaftTestInteger(hello, 1, 4);
        appendRaftTestInteger(hello, sender);
        const auto clusterId = config.clusterId();
        hello.insert(hello.end(), clusterId.begin(), clusterId.end());
        hello.insert(hello.end(), fingerprint.begin(), fingerprint.end());
        hello.push_back(static_cast<uint8_t>(RaftBlobPolicy::REJECT)); hello.push_back(0);
        appendRaftTestInteger(hello, ClusterRuntimeOptions{}.requests.maxRetentionMs);
        appendRaftTestInteger(hello, ClusterRuntimeOptions{}.requests.maxTrackedRequests, 4);
        AKK_CLUSTER_CHECK(socket.send(encodeFrame(ReplMsgType::RAFT_PEER_HELLO, hello)));
        const auto response = socket.receiveFrame();
        AKK_CLUSTER_CHECK(response.type == ReplMsgType::RAFT_PEER_HELLO_RESPONSE && response.payload.size() == 1);
        AKK_CLUSTER_CHECK(response.payload[0] == (domain == '1' ? 0 : 3));
    }

    DecodedFrame probeRaftTestPeer(const RaftTestSocket& socket, uint64_t term, uint64_t candidate, uint64_t index, uint64_t logTerm,
        ReplMsgType type = ReplMsgType::RAFT_PRE_VOTE) {
        std::vector<uint8_t> request;
        for (uint64_t value : {term, candidate, index, logTerm}) { appendRaftTestInteger(request, value); }
        AKK_CLUSTER_CHECK(socket.send(encodeFrame(type, request)));
        return socket.receiveFrame();
    }

    void testRaftPreVoteWireContract() {
        const auto dir = makeTempDir("raft-prevote-wire");
        auto raft = onlineRaftMembership();
        raft.membership.allowLearners = true;
        const ClusterConfig config{{node(1, 24701, 24711), node(2, 24702, 24712), node(3, 24703, 24713),
            node(4, 24704, 24714, DATA_AND_COORDINATOR | static_cast<uint32_t>(NodeCapability::RAFT_LEARNER))},
            ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 1000}, raft};
        auto n1 = makeRuntime(dir / "n1", config, 1);
        n1->runtime->start();
        RaftTestSocket voter, learner, oldProtocol;
        openRaftTestPeer(voter, 24711, 2, config);
        openRaftTestPeer(learner, 24711, 4, config);
        openRaftTestPeer(oldProtocol, 24711, 3, config, '2');
        auto check = [](const DecodedFrame& response, uint64_t term, bool granted) {
            AKK_CLUSTER_CHECK(response.type == ReplMsgType::RAFT_PRE_VOTE_RESPONSE && response.payload.size() == 9);
            AKK_CLUSTER_CHECK(readU64Le(response.payload, 0) == term && response.payload[8] == static_cast<uint8_t>(granted));
        };
        const auto before = n1->runtime->raftStats();
        check(probeRaftTestPeer(voter, 5000, 2, 0, 0), before.currentTerm, true);
        check(probeRaftTestPeer(voter, 0, 2, 0, 0), before.currentTerm, false);
        check(probeRaftTestPeer(voter, 5000, 3, 0, 0), 0, false); // Cannot impersonate another hello identity.
        check(probeRaftTestPeer(learner, 5000, 4, 0, 0), before.currentTerm, false);
        // Truncated and extended requests must not be interpreted as valid votes.
        for (size_t size : {size_t{31}, size_t{33}}) {
            AKK_CLUSTER_CHECK(voter.send(encodeFrame(ReplMsgType::RAFT_PRE_VOTE, std::vector<uint8_t>(size, 0xFF))));
            check(voter.receiveFrame(), 0, false);
        }
        AKK_CLUSTER_CHECK(n1->runtime->raftStats().currentTerm == before.currentTerm && n1->runtime->role() == NodeRole::REPLICA);
        // A real vote advances hard state; later probes must report that actual term.
        const auto vote = probeRaftTestPeer(voter, 1, 2, 0, 0, ReplMsgType::RAFT_REQUEST_VOTE);
        AKK_CLUSTER_CHECK(vote.type == ReplMsgType::RAFT_REQUEST_VOTE_RESPONSE && vote.payload[8] == 1);
        check(probeRaftTestPeer(voter, 1, 2, 0, 0), 1, false);
        check(probeRaftTestPeer(voter, 5000, 2, 0, 0), 1, true);
        RaftTestSocket otherVoter;
        openRaftTestPeer(otherVoter, 24711, 3, config);
        check(probeRaftTestPeer(otherVoter, 5000, 3, 0, 0), 1, true);
        const auto deniedVote = probeRaftTestPeer(otherVoter, 1, 3, 0, 0, ReplMsgType::RAFT_REQUEST_VOTE);
        AKK_CLUSTER_CHECK(deniedVote.type == ReplMsgType::RAFT_REQUEST_VOTE_RESPONSE && deniedVote.payload[8] == 0);
        // A same-term heartbeat protects a live leader, even from an enormous
        // prospective term. Receiving the probe must not refresh that protection.
        std::vector<uint8_t> heartbeat;
        for (uint64_t value : {uint64_t{1}, uint64_t{2}, uint64_t{0}, uint64_t{0}, uint64_t{0}}) { appendRaftTestInteger(heartbeat, value); }
        appendRaftTestInteger(heartbeat, 0, 4);
        AKK_CLUSTER_CHECK(voter.send(encodeFrame(ReplMsgType::RAFT_APPEND_ENTRIES, heartbeat)));
        AKK_CLUSTER_CHECK(voter.receiveFrame().type == ReplMsgType::RAFT_APPEND_ENTRIES_RESPONSE);
        check(probeRaftTestPeer(otherVoter, UINT64_MAX, 3, 0, 0), 1, false);
        const auto protectionExpires = std::chrono::steady_clock::now() + std::chrono::milliseconds{3100};
        while (std::chrono::steady_clock::now() < protectionExpires) {
            (void)probeRaftTestPeer(otherVoter, UINT64_MAX, 3, 0, 0);
            std::this_thread::sleep_for(std::chrono::milliseconds{25});
        }
        check(probeRaftTestPeer(otherVoter, UINT64_MAX, 3, 0, 0), 1, true);
        otherVoter.shutdown();
        voter.shutdown(); learner.shutdown(); oldProtocol.shutdown();
        n1->runtime->close();
        n1.reset();
        n1 = makeRuntime(dir / "n1", config, 1);
        AKK_CLUSTER_CHECK(n1->runtime->raftStats().currentTerm == 1); // Probes never persist their prospective term.
    }

    void testRaftPreVoteRejectsStaleLog() {
        const auto dir = makeTempDir("raft-prevote-stale-log");
        const ClusterConfig config{{node(1, 24741, 24751), node(2, 24742, 24752), node(3, 24743, 24753)},
            ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 1000}};
        auto n1 = makeRuntime(dir / "n1", config, 1), n2 = makeRuntime(dir / "n2", config, 2);
        n1->runtime->start(); n2->runtime->start();
        RuntimeHarness* majority[] = {n1.get(), n2.get()};
        AKK_CLUSTER_CHECK(waitUntil([&] { return findLeader(majority) == n1.get(); }, std::chrono::seconds{12}));
        n1->runtime->shipEntry(1, ReplOpType::PUT, bytesOf("prevote-log"), bytesOf("committed"), 0, 1);
        AKK_CLUSTER_CHECK(waitUntil([&] { return n2->hasState(1, "prevote-log", "committed"); }));
        RaftTestSocket peer;
        openRaftTestPeer(peer, 24752, 3, config);
        const auto term = n1->runtime->raftStats().currentTerm;
        auto response = probeRaftTestPeer(peer, term + 10, 3, UINT64_MAX, term);
        AKK_CLUSTER_CHECK(response.payload[8] == 0); // Healthy follower refuses even a fresh log.
        n1->runtime->close();
        std::this_thread::sleep_for(std::chrono::milliseconds{3100});
        response = probeRaftTestPeer(peer, term + 10, 3, 0, 0);
        AKK_CLUSTER_CHECK(readU64Le(response.payload, 0) == term && response.payload[8] == 0);
        response = probeRaftTestPeer(peer, term + 10, 3, UINT64_MAX, term);
        AKK_CLUSTER_CHECK(readU64Le(response.payload, 0) == term && response.payload[8] == 1);
        AKK_CLUSTER_CHECK(n2->runtime->raftStats().currentTerm == term && n2->runtime->role() == NodeRole::REPLICA);
        peer.shutdown(); n2->runtime->close();
    }

    void testRaftPreVoteDiscardsProbeAfterHeartbeat() {
        const auto dir = makeTempDir("raft-prevote-heartbeat-race");
        const ClusterConfig config{{node(1, 24761, 24771), node(2, 24762, 24772), node(3, 24763, 24773)},
            ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 1000}};
        RaftTestSocket listener;
        listener.listen(24772);
        std::promise<void> probeEntered, releaseProbe;
        auto entered = probeEntered.get_future();
        auto release = releaseProbe.get_future();
        auto fakeVoter = std::async(std::launch::async, [&] {
            RaftTestSocket connection{::accept(listener.handle, nullptr, nullptr)};
            AKK_CLUSTER_CHECK(connection.handle != RaftTestSocket::INVALID);
            const auto hello = connection.receiveFrame();
            AKK_CLUSTER_CHECK(hello.type == ReplMsgType::RAFT_PEER_HELLO);
            AKK_CLUSTER_CHECK(connection.send(encodeFrame(ReplMsgType::RAFT_PEER_HELLO_RESPONSE, std::array<uint8_t, 1>{0})));
            const auto probe = connection.receiveFrame();
            AKK_CLUSTER_CHECK(probe.type == ReplMsgType::RAFT_PRE_VOTE && readU64Le(probe.payload, 0) == 1);
            probeEntered.set_value();
            AKK_CLUSTER_CHECK(release.wait_for(std::chrono::seconds{5}) == std::future_status::ready);
            std::vector<uint8_t> response;
            appendRaftTestInteger(response, 0); response.push_back(1);
            AKK_CLUSTER_CHECK(connection.send(encodeFrame(ReplMsgType::RAFT_PRE_VOTE_RESPONSE, response)));
            // Keep the successful probe connection alive until the runtime closes it.
            std::array<uint8_t, 1> byte{};
            (void)connection.receive(byte);
        });
        auto n1 = makeRuntime(dir / "n1", config, 1);
        n1->runtime->start();
        RaftTestSocket leader;
        openRaftTestPeer(leader, 24771, 3, config);
        AKK_CLUSTER_CHECK(entered.wait_for(std::chrono::seconds{7}) == std::future_status::ready);
        std::vector<uint8_t> heartbeat;
        for (uint64_t value : {uint64_t{0}, uint64_t{3}, uint64_t{0}, uint64_t{0}, uint64_t{0}}) { appendRaftTestInteger(heartbeat, value); }
        appendRaftTestInteger(heartbeat, 0, 4);
        AKK_CLUSTER_CHECK(leader.send(encodeFrame(ReplMsgType::RAFT_APPEND_ENTRIES, heartbeat)));
        const auto heartbeatResponse = leader.receiveFrame();
        AKK_CLUSTER_CHECK(heartbeatResponse.type == ReplMsgType::RAFT_APPEND_ENTRIES_RESPONSE && heartbeatResponse.payload[8] == 1);
        releaseProbe.set_value();
        // Wait for the completed vote-probe round trip, so a timeout cannot
        // masquerade as safe rejection. Forward RPCs have independent channels.
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto peers = n1->runtime->raftStats().peers;
            const auto peer = std::ranges::find(peers, uint64_t{2}, &RaftPeerStats::nodeId);
            return peer != peers.end() && peer->roundTripsSucceeded != 0;
        }, std::chrono::seconds{2}));
        const auto observeUntil = std::chrono::steady_clock::now() + std::chrono::milliseconds{1000};
        while (std::chrono::steady_clock::now() < observeUntil) {
            AKK_CLUSTER_CHECK(n1->runtime->raftStats().currentTerm == 0 && n1->runtime->role() == NodeRole::REPLICA);
            std::this_thread::sleep_for(std::chrono::milliseconds{25});
        }
        leader.shutdown(); n1->runtime->close();
        fakeVoter.get();
    }

    void testRaftPreVoteIsolationAndRecovery(bool secure = false) {
        const auto dir = makeTempDir(secure ? "raft-prevote-isolation-secure" : "raft-prevote-isolation");
        const ClusterConfig config{{node(1, 24721, 24731), node(2, 24722, 24732), node(3, 24723, 24733)},
            ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 1000}};
        ClusterRuntimeOptions options;
        options.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        if (secure) {
            for (uint64_t id = 1; id <= 3; ++id) {
                const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
                options.secure.pinnedPeers.push_back({id, identity.publicKey});
            }
        }
        auto makePeer = [&](uint64_t id) {
            auto peerOptions = options;
            if (id == 3) { peerOptions.replBindHost = "127.0.0.2"; }
            peerOptions.secure.identitySeedPath = dir / ("identity-" + std::to_string(id));
            return makeRuntime(dir / ("n" + std::to_string(id)), config, id, peerOptions, secure);
        };
        auto n1 = makePeer(1), n2 = makePeer(2), n3 = makePeer(3);
        RaftInboundFaultProxy proxy{24733};
        n3->runtime->start();
        // Observe more than two election deadlines while every other voter is offline.
        const auto isolatedUntil = std::chrono::steady_clock::now() + std::chrono::milliseconds{7500};
        while (std::chrono::steady_clock::now() < isolatedUntil) {
            AKK_CLUSTER_CHECK(n3->runtime->raftStats().currentTerm == 0 && n3->runtime->role() == NodeRole::REPLICA);
            std::this_thread::sleep_for(std::chrono::milliseconds{25});
        }
        n3->runtime->close(); n3.reset();
        // Establish the majority's leader before letting the cut node campaign.
        // During initial startup, that node could legitimately win with its
        // working outbound links, which is not the failure this test targets.
        n1->runtime->start(); n2->runtime->start();
        RuntimeHarness* majority[] = {n1.get(), n2.get()};
        ensurePreferredLeader(majority, std::chrono::seconds{12});
        n1->runtime->shipEntry(1, ReplOpType::PUT, bytesOf("prevote"), bytesOf("before"), 0, 1);
        AKK_CLUSTER_CHECK(waitUntil([&] { return n2->hasState(1, "prevote", "before"); }));
        const auto stableTerm = n1->runtime->raftStats().currentTerm;
        n3 = makePeer(3);
        AKK_CLUSTER_CHECK(n3->runtime->raftStats().currentTerm == 0);
        n3->runtime->start();
        // The cut follower can contact the majority but receives no heartbeats.
        const auto campaignUntil = std::chrono::steady_clock::now() + std::chrono::milliseconds{7500};
        while (std::chrono::steady_clock::now() < campaignUntil) {
            AKK_CLUSTER_CHECK(n1->runtime->role() == NodeRole::PRIMARY && n1->runtime->raftStats().currentTerm == stableTerm);
            AKK_CLUSTER_CHECK(n2->runtime->raftStats().currentTerm == stableTerm && n3->runtime->raftStats().currentTerm <= stableTerm);
            std::this_thread::sleep_for(std::chrono::milliseconds{25});
        }
        proxy.setBlocked(false);
        AKK_CLUSTER_CHECK(waitUntil([&] { return n3->hasState(1, "prevote", "before"); }, std::chrono::seconds{8}));
        AKK_CLUSTER_CHECK(n1->runtime->role() == NodeRole::PRIMARY && n1->runtime->raftStats().currentTerm == stableTerm);
        n1->runtime->shipEntry(2, ReplOpType::PUT, bytesOf("prevote"), bytesOf("rejoined"), 0, 1);
        AKK_CLUSTER_CHECK(waitUntil([&] { return n3->hasState(2, "prevote", "rejoined"); }));
        n1->runtime->close();
        RuntimeHarness* survivors[] = {n2.get(), n3.get()};
        AKK_CLUSTER_CHECK(waitUntil([&] { return findLeader(survivors) != nullptr; }, std::chrono::seconds{12}));
        auto* replacement = findLeader(survivors);
        AKK_CLUSTER_CHECK(replacement != nullptr && replacement->runtime->raftStats().currentTerm > stableTerm);
        replacement->runtime->shipEntry(3, ReplOpType::PUT, bytesOf("prevote"), bytesOf("after-failure"), 0, replacement->nodeId);
        AKK_CLUSTER_CHECK(waitUntil([&] { return n2->hasState(3, "prevote", "after-failure") && n3->hasState(3, "prevote", "after-failure"); }));
        AKK_CLUSTER_CHECK(replacement->runtime->readKey(bytesOf("prevote"), 0).status == ReadStatus::FOUND);
        AKK_CLUSTER_CHECK(!proxy.failed());
        n3->runtime->close(); n2->runtime->close();
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

        ClusterRuntimeOptions options;
        options.raftHeartbeatIntervalMs = 500;
        auto n1 = makeRuntime(dir / "n1", cfg, 1, options);
        auto n2 = makeRuntime(dir / "n2", cfg, 2, options);
        auto n3 = makeRuntime(dir / "n3", cfg, 3, options);

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

    void testFileBackedSnapshotTransfer() {
        namespace transfer = akkaradb::engine::cluster::detail;
        const auto dir = makeTempDir("file-backed-snapshot-transfer");
        const auto exportPath = dir / "snapshot-export";
        const std::string key = "file-backed-key";
        const std::string value(512 * 1024, 'f');
        const std::string padding = "export-metadata";
        std::ofstream exportOut{exportPath, std::ios::binary | std::ios::trunc};
        exportOut.write(padding.data(), static_cast<std::streamsize>(padding.size()));
        exportOut.write(key.data(), static_cast<std::streamsize>(key.size()));
        exportOut.write(value.data(), static_cast<std::streamsize>(value.size()));
        exportOut.flush();
        AKK_CLUSTER_CHECK(static_cast<bool>(exportOut));

        std::vector<uint8_t> expected;
        expected.reserve(8 + key.size() + value.size());
        for (size_t i = 0; i < 4; ++i) { expected.push_back(static_cast<uint8_t>(key.size() >> (i * 8))); }
        for (size_t i = 0; i < 4; ++i) { expected.push_back(static_cast<uint8_t>(value.size() >> (i * 8))); }
        expected.insert(expected.end(), key.begin(), key.end());
        expected.insert(expected.end(), value.begin(), value.end());

        ReplicationTransferOptions options;
        options.thresholdBytes = 128;
        options.chunkBytes = 4096;
        options.maxMemoryBytes = 2 * 1024 * 1024;
        options.maxSpoolBytes = 4 * 1024 * 1024;
        options.spoolDirectory = dir / "transfer-spools";
        auto sender = std::make_shared<transfer::TransferBudget>(options);
        auto receiver = std::make_shared<transfer::TransferBudget>(options);
        auto exportOwner = std::shared_ptr<void>{new int{1}, [exportPath](void* value) {
            delete static_cast<int*>(value);
            std::error_code ignored;
            std::filesystem::remove(exportPath, ignored);
        }};
        auto message = transfer::snapshotFileMessage(SnapshotFileEntry{
            .path = exportPath,
            .dataOffset = padding.size(),
            .keySize = static_cast<uint32_t>(key.size()),
            .valueSize = value.size(),
            .payloadCrc32c = akkaradb::cpu::CRC32C(
                reinterpret_cast<const std::byte*>(expected.data()), expected.size()),
            .storage = exportOwner,
        }, sender);
        exportOwner.reset();
        AKK_CLUSTER_CHECK(std::filesystem::exists(exportPath));
        AKK_CLUSTER_CHECK(message->payload.empty());
        AKK_CLUSTER_CHECK(message->payloadSize() == expected.size());
        AKK_CLUSTER_CHECK(sender->used(transfer::TransferBudget::Resource::SPOOL) == 0);

        std::vector<std::vector<uint8_t>> frames;
        AKK_CLUSTER_CHECK(transfer::sendMessage(*message, sender, [&](std::span<const uint8_t> wire) {
            frames.emplace_back(wire.begin(), wire.end());
            return true;
        }));
        size_t frameIndex = 0;
        const auto received = transfer::receiveMessage(receiver, [&](DecodedFrame& frame) {
            return frameIndex < frames.size() && decodeFrame(frames[frameIndex++], frame);
        });
        std::span<const uint8_t> receivedKey;
        std::span<const uint8_t> receivedValue;
        AKK_CLUSTER_CHECK(received && transfer::snapshotView(received->payload, receivedKey, receivedValue));
        AKK_CLUSTER_CHECK(textOf(receivedKey) == key && textOf(receivedValue) == value);

        auto session = std::make_shared<transfer::TransferSession>();
        std::mutex framesMutex;
        std::vector<std::vector<uint8_t>> resumedFrames;
        const uint64_t resumeOffset = expected.size() / 2;
        auto sending = std::async(std::launch::async, [&] {
            return transfer::sendMessage(*message, sender, session, [&](std::span<const uint8_t> wire) {
                std::lock_guard lock{framesMutex};
                resumedFrames.emplace_back(wire.begin(), wire.end());
                return true;
            });
        });
        AKK_CLUSTER_CHECK(waitUntil([&] { std::lock_guard lock{framesMutex}; return !resumedFrames.empty(); }));
        DecodedFrame begin;
        {
            std::lock_guard lock{framesMutex};
            AKK_CLUSTER_CHECK(decodeFrame(resumedFrames.front(), begin));
        }
        transfer::TransferId id{};
        std::copy_n(begin.payload.begin() + 14, id.size(), id.begin());
        AKK_CLUSTER_CHECK(session->notifyReady(id, resumeOffset));
        AKK_CLUSTER_CHECK(sending.get());
        DecodedFrame firstChunk;
        {
            std::lock_guard lock{framesMutex};
            AKK_CLUSTER_CHECK(resumedFrames.size() > 2 && decodeFrame(resumedFrames[1], firstChunk));
        }
        AKK_CLUSTER_CHECK(firstChunk.type == ReplMsgType::TRANSFER_CHUNK && readU64Le(firstChunk.payload, 0) == resumeOffset);
        AKK_CLUSTER_CHECK(std::equal(firstChunk.payload.begin() + 8, firstChunk.payload.end(),
            expected.begin() + static_cast<std::ptrdiff_t>(resumeOffset)));
        AKK_CLUSTER_CHECK(sender->used(transfer::TransferBudget::Resource::SPOOL) == 0);
        exportOut.write("next-entry", 10);
        exportOut.flush();
        AKK_CLUSTER_CHECK(static_cast<bool>(exportOut));
        exportOut.close();
        message.reset();
        AKK_CLUSTER_CHECK(!std::filesystem::exists(exportPath));
    }

    void testMemoryOnlySnapshotModes() {
        using akkaradb::engine::memtable::MemTable;
        MemTable::Options options;
        options.shardCount = 1;
        options.flushMode = akkaradb::engine::memtable::MemTableFlushMode::MANUAL_ONLY;
        options.thresholdBytesPerShard = 0;
        auto table = MemTable::create(options);
        table->put(bytesOf("key"), bytesOf("old"), 1);
        table->put(bytesOf("removed"), bytesOf("present"), 2);

        MemTable::KeyRange range;
        auto first = table->sealAndPinMemoryIterator(range, 2, true, 16 * 1024 * 1024, 1);
        AKK_CLUSTER_CHECK(first.has_value());
        table->put(bytesOf("key"), bytesOf("new"), 3);
        table->remove(bytesOf("removed"), 4);
        table->put(bytesOf("added"), bytesOf("later"), 5);

        std::vector<std::pair<std::string, std::string>> snapshotRows;
        while (first->hasNext()) {
            const auto record = first->next();
            AKK_CLUSTER_CHECK(record.has_value());
            snapshotRows.emplace_back(textOf(record->key()), textOf(record->value()));
        }
        const std::vector<std::pair<std::string, std::string>> expectedRows{
            {"key", "old"}, {"removed", "present"}
        };
        AKK_CLUSTER_CHECK(snapshotRows == expectedRows);

        akkaradb::core::RecordView current;
        AKK_CLUSTER_CHECK(table->get(bytesOf("key"), 5, &current) && textOf(current.value()) == "new");
        AKK_CLUSTER_CHECK(table->get(bytesOf("removed"), 5, &current) && current.isTombstone());

        // THROUGHPUT_FIRST never waits behind an existing pin or an
        // undersized pinned-byte budget.
        AKK_CLUSTER_CHECK(!table->sealAndPinMemoryIterator(range, 5, false, 16 * 1024 * 1024, 1));
        first.reset();
        AKK_CLUSTER_CHECK(!table->sealAndPinMemoryIterator(range, 5, false, 1, 1));

        auto blocker = table->sealAndPinMemoryIterator(range, 5, true, 16 * 1024 * 1024, 1);
        AKK_CLUSTER_CHECK(blocker.has_value());
        auto waiting = std::async(std::launch::async, [&] {
            return table->sealAndPinMemoryIterator(range, 5, true, 16 * 1024 * 1024, 1);
        });
        AKK_CLUSTER_CHECK(waiting.wait_for(std::chrono::milliseconds{50}) == std::future_status::timeout);
        blocker.reset();
        auto completed = waiting.get();
        AKK_CLUSTER_CHECK(completed.has_value());
        completed.reset();

        AKK_CLUSTER_CHECK(table->get(bytesOf("key"), 5, &current) && textOf(current.value()) == "new");
        AKK_CLUSTER_CHECK(table->get(bytesOf("removed"), 5, &current) && current.isTombstone());
        AKK_CLUSTER_CHECK(waitUntil([&] { return table->snapshot().immutableTables <= 1; }));

        uint64_t sequence = 5;
        for (unsigned cycle = 0; cycle < 8; ++cycle) {
            auto pinned = table->sealAndPinMemoryIterator(range, sequence, true, 16 * 1024 * 1024, 1);
            AKK_CLUSTER_CHECK(pinned.has_value());
            table->put(bytesOf("key"), bytesOf(std::to_string(cycle)), ++sequence);
            pinned.reset();
            AKK_CLUSTER_CHECK(waitUntil([&] { return table->snapshot().immutableTables <= 1; }));
        }

        std::atomic<bool> rejectBackends{false};
        MemTable::Options retryOptions = options;
        retryOptions.backendFactory = [&]() -> std::unique_ptr<akkaradb::engine::memtable::IMemTable> {
            if (rejectBackends.load(std::memory_order_acquire)) { return nullptr; }
            return std::make_unique<akkaradb::engine::memtable::SkipListMemTable>();
        };
        auto retryTable = MemTable::create(retryOptions);
        retryTable->put(bytesOf("retry"), bytesOf("old"), 1);
        auto retrySnapshot = retryTable->sealAndPinMemoryIterator(range, 1, true, 16 * 1024 * 1024, 1);
        AKK_CLUSTER_CHECK(retrySnapshot.has_value());
        retryTable->put(bytesOf("retry"), bytesOf("new"), 2);
        rejectBackends.store(true, std::memory_order_release);
        retrySnapshot.reset();
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto stats = retryTable->snapshot();
            return stats.memorySnapshotCompactionFailures != 0 && stats.memorySnapshotCompactionPending &&
                stats.memorySnapshotCompactionLastFailureAtUs != 0;
        }));
        AKK_CLUSTER_CHECK(retryTable->get(bytesOf("retry"), 2, &current) && textOf(current.value()) == "new");
        rejectBackends.store(false, std::memory_order_release);
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto stats = retryTable->snapshot();
            return stats.memorySnapshotCompactionsCompleted != 0 && !stats.memorySnapshotCompactionPending &&
                stats.immutableTables <= 1;
        }));
        auto retryThroughput = retryTable->sealAndPinMemoryIterator(range, 2, false, 16 * 1024 * 1024, 1);
        AKK_CLUSTER_CHECK(retryThroughput.has_value());
        retryThroughput.reset();

        auto closingTable = MemTable::create(options);
        closingTable->put(bytesOf("close"), bytesOf("safe"), 1);
        auto survivesClose = closingTable->sealAndPinMemoryIterator(range, 1, true, 16 * 1024 * 1024, 1);
        AKK_CLUSTER_CHECK(survivesClose.has_value());
        closingTable.reset();
        AKK_CLUSTER_CHECK(survivesClose->hasNext());
        const auto closeRecord = survivesClose->next();
        AKK_CLUSTER_CHECK(closeRecord && textOf(closeRecord->value()) == "safe");
        survivesClose.reset();
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
        AKK_CLUSTER_CHECK(waitUntil([&] {
            return server->replicaCount() == 2 && std::ranges::all_of(clients, [](const auto& client) { return client->connected(); });
        }));
        AKK_CLUSTER_CHECK(server->stats().connectedReplicas == 2);
        std::vector<uint64_t> initialContacts;
        initialContacts.reserve(clients.size());
        for (const auto& client : clients) {
            AKK_CLUSTER_CHECK(client->lastSuccessfulContactAtUs() != 0);
            initialContacts.push_back(client->lastSuccessfulContactAtUs());
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{1'500});
        AKK_CLUSTER_CHECK(server->replicaCount() == 2);
        for (size_t i = 0; i < clients.size(); ++i) {
            AKK_CLUSTER_CHECK(clients[i]->connected());
            AKK_CLUSTER_CHECK(clients[i]->lastSuccessfulContactAtUs() == initialContacts[i]);
        }
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
        options.replicationClusterId = cfg.clusterId();
        options.replicationConfigFingerprint = cfg.replicationFingerprint();

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

    void testClusterRuntimeRoleChangeDoesNotJoinEndpointUnderStateLock() {
        ScopedPrimaryStepDown primaryStepDown;
        const auto dir = makeTempDir("runtime-role-change-lock");
        const ClusterConfig cfg{
            {node(1, 23661, 23671), node(2, 23662, 23672)},
            ReplicationMode::MIRROR,
            AckPolicy{},
            ConsistencyOptions{.mode = ConsistencyMode::PRIMARY_ACK, .ackTimeoutMs = 1000},
        };
        ClusterRuntimeOptions options;
        options.transportMode = TransportMode::PLAIN;
        options.startupRole = NodeStartupRole::PRIMARY;
        options.clusterGroupId = 9025;
        options.clusterGroupEpoch = 1;
        options.replicationClusterId = cfg.clusterId();
        options.replicationConfigFingerprint = cfg.replicationFingerprint();

        ClusterRuntime* runtimeView = nullptr;
        std::promise<void> readEntered;
        auto readEnteredFuture = readEntered.get_future();
        std::promise<void> releaseRead;
        auto readReleased = releaseRead.get_future().share();
        std::promise<void> roleChangeFinished;
        auto roleChangeFinishedFuture = roleChangeFinished.get_future();
        std::atomic<bool> replicaRoleReported{false};

        ClusterEngineCallbacks callbacks;
        callbacks.getCurrentSeq = [] { return uint64_t{0}; };
        callbacks.getLastSeq = [] { return uint64_t{0}; };
        callbacks.read = [&](std::span<const uint8_t>, uint64_t) {
            readEntered.set_value();
            readReleased.wait();
            (void)runtimeView->raftStats();
            ReadResponse response;
            response.status = ReadStatus::NOT_FOUND;
            return response;
        };
        callbacks.apply = [](uint64_t, ReplOpType, std::span<const uint8_t>, std::span<const uint8_t>, uint8_t, uint64_t, uint64_t) {};
        callbacks.forceDurable = [] {};
        callbacks.roleChange = [&](NodeRole role) {
            if (role == NodeRole::REPLICA && !replicaRoleReported.exchange(true)) { roleChangeFinished.set_value(); }
        };

        auto runtime = ClusterRuntime::create(dir, cfg, 1, std::move(callbacks), options);
        runtimeView = runtime.get();
        runtime->start();

        auto client = ReplicationClient::create("127.0.0.1", 23671, 2, [] { return uint64_t{0}; }, {}, options);
        client->setApplyCallback([](uint64_t, ReplOpType, std::span<const uint8_t>, std::span<const uint8_t>, uint8_t, uint64_t) {});
        client->setForceDurableCallback([] {});
        client->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return client->connected(); }));
        auto read = std::async(std::launch::async, [&] { return client->readKey(bytesOf("blocked"), 0, 5000); });
        AKK_CLUSTER_CHECK(readEnteredFuture.wait_for(std::chrono::seconds{1}) == std::future_status::ready);

        primaryStepDown.trigger();
        const bool steppedDown = waitUntil([&] { return runtime->role() == NodeRole::REPLICA; }, std::chrono::seconds{2});
        if (steppedDown) { std::this_thread::sleep_for(std::chrono::milliseconds{50}); }
        releaseRead.set_value();

        AKK_CLUSTER_CHECK(steppedDown);
        AKK_CLUSTER_CHECK(roleChangeFinishedFuture.wait_for(std::chrono::seconds{2}) == std::future_status::ready);
        AKK_CLUSTER_CHECK(read.wait_for(std::chrono::seconds{2}) == std::future_status::ready);
        try { (void)read.get(); } catch (const std::runtime_error&) {}
        client->close();
        runtime->close();
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
        options.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
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
        options.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
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
        std::vector<akkaradb::engine::detail::BulkPutEntry> batch;
        const std::string batchValue = "batch-value";
        for (const auto& key : keys) { batch.push_back({bytesOf(key), bytesOf(batchValue)}); }
        akkaradb::engine::detail::ProtocolBulkWriter::put(*engine, batch);
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
        blob::BlobManager::Options blobOptions;
        blobOptions.codec = blob::BlobCodec::ZSTD;
        auto manager = blob::BlobManager::create(dir, blobOptions);
        manager->start();
        std::vector<uint8_t> expected(2u * 1024u * 1024u + 257u);
        for (size_t i = 0; i < expected.size(); ++i) { expected[i] = static_cast<uint8_t>((i / 17u) % 31u); }
        manager->write(7, expected);
        const auto path = manager->blobPath(7);
        AKK_CLUSTER_CHECK(std::filesystem::exists(path));

        std::vector<uint8_t> streamed;
        streamed.reserve(expected.size());
        uint64_t expectedOffset = 0;
        size_t chunks = 0;
        AKK_CLUSTER_CHECK(manager->streamRead(
            7,
            blob::crc32c(expected),
            64u * 1024u,
            [&](uint64_t offset, std::span<const uint8_t> chunk) {
                AKK_CLUSTER_CHECK(offset == expectedOffset);
                AKK_CLUSTER_CHECK(chunk.size() <= 64u * 1024u);
                streamed.insert(streamed.end(), chunk.begin(), chunk.end());
                expectedOffset += chunk.size();
                ++chunks;
                return true;
            }
        ));
        AKK_CLUSTER_CHECK(chunks > 1);
        AKK_CLUSTER_CHECK(streamed == expected);

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

    void testRaftLearnerValidationAndStaticMembership() {
        const auto dir = makeTempDir("raft-learner-validation");
        auto learner = node(2, 23132, 23232);
        learner.capabilities |= RAFT_LEARNER;
        const auto rejects = [](const auto& operation) {
            bool rejected = false;
            try { operation(); } catch (const std::exception&) { rejected = true; }
            AKK_CLUSTER_CHECK(rejected);
        };
        const auto voter = node(1, 23131, 23231);
        const ConsistencyOptions consistency{.mode = ConsistencyMode::RAFT_QUORUM};
        rejects([&] { (void)ClusterConfig{{voter, learner}, ReplicationMode::MIRROR, {}, consistency}; });
        RaftOptions raft;
        raft.membership.allowLearners = true;
        rejects([&] { (void)ClusterConfig{{learner}, ReplicationMode::MIRROR, {}, consistency, raft}; });
        rejects([&] { (void)ClusterConfig{{voter, learner}, ReplicationMode::MIRROR, {}, {}, raft}; });
        auto noDataLearner = learner;
        noDataLearner.capabilities &= ~static_cast<uint32_t>(DATA_BEARING);
        rejects([&] { (void)ClusterConfig{{voter, noDataLearner}, ReplicationMode::MIRROR, {}, consistency, raft}; });
        const ClusterConfig disabled{{voter}, ReplicationMode::MIRROR, {}, consistency};
        auto runtime = makeRuntime(dir / "disabled", disabled, 1);
        runtime->runtime->start();
        rejects([&] { runtime->runtime->addRaftLearner(learner); });
        rejects([&] { runtime->runtime->removeRaftLearner(2); });
        rejects([&] { runtime->runtime->promoteRaftLearner(2); });
        runtime->runtime->close();
        const ClusterConfig enabled{{voter}, ReplicationMode::MIRROR, {}, consistency, raft};
        runtime = makeRuntime(dir / "static", enabled, 1);
        runtime->runtime->start();
        runtime->runtime->addRaftLearner(learner);
        AKK_CLUSTER_CHECK(runtime->runtime->raftStats().learnerCount == 1);
        rejects([&] { runtime->runtime->promoteRaftLearner(2); });
        runtime->runtime->removeRaftLearner(2);
        AKK_CLUSTER_CHECK(runtime->runtime->raftStats().voterCount == 1);
        AKK_CLUSTER_CHECK(runtime->runtime->raftStats().learnerCount == 0);
        runtime->runtime->close();
    }

    void testRaftLearnerEngineApi() {
        const auto dir = makeTempDir("raft-learner-engine");
        auto raft = onlineRaftMembership();
        raft.membership.allowLearners = true;
        const auto voter = node(1, 23141, 23241);
        auto learner = node(2, 23142, 23242);
        learner.capabilities |= RAFT_LEARNER;
        const ConsistencyOptions consistency{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 1500};
        const ClusterConfig initial{{voter}, ReplicationMode::MIRROR, {}, consistency, raft};
        const ClusterConfig joining{{voter, learner}, ReplicationMode::MIRROR, {}, consistency, raft, {}, 0, initial.clusterId()};
        const auto makeOptions = [&](uint64_t id, const ClusterConfig& config) {
            akkaradb::engine::AkkEngineOptions options;
            options.paths.dataDir = dir / ("n" + std::to_string(id));
            options.components.clusterEnabled = true;
            options.components.blobEnabled = false;
            options.cluster.config = config;
            options.cluster.runtime.transportMode = TransportMode::PLAIN;
            options.cluster.runtime.readMode = ClusterReadMode::LOCAL_STALE_OK;
            writeNodeIdFile(options.paths.dataDir / "node.id", id);
            return options;
        };
        auto n1 = akkaradb::engine::AkkEngine::open(makeOptions(1, initial));
        auto n2 = akkaradb::engine::AkkEngine::open(makeOptions(2, joining));
        AKK_CLUSTER_CHECK(waitUntil([&] { return n1->stats().cluster.role == static_cast<uint32_t>(NodeRole::PRIMARY); }));
        n1->addClusterLearner(learner);
        n1->put(bytesOf("learner-api"), bytesOf("replicated"));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto value = engineGet(*n2, "learner-api");
            return value && textOf(*value) == "replicated" && n2->stats().cluster.localLearner;
        }));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto stats = n1->stats().cluster;
            return std::ranges::any_of(stats.peers, [&](const auto& peer) {
                return peer.nodeId == 2 && peer.learner && peer.matchIndex == stats.lastLogIndex;
            });
        }));
        n1->promoteClusterLearner(2);
        AKK_CLUSTER_CHECK(waitUntil([&] { return n2->stats().cluster.voterCount == 2 && !n2->stats().cluster.localLearner; }));
        n1->put(bytesOf("learner-api"), bytesOf("promoted"));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto value = engineGet(*n2, "learner-api");
            return value && textOf(*value) == "promoted";
        }));
        auto offline = node(3, 23143, 23243);
        n1->addClusterLearner(offline);
        n1->removeClusterLearner(3);
        AKK_CLUSTER_CHECK(n1->stats().cluster.learnerCount == 0);
        n2->close();
        n1->close();
    }

    ClusterConfig learnerCrashConfig() {
        auto raft = onlineRaftMembership();
        raft.membership.allowLearners = true;
        auto learner = node(2, 23122, 23222);
        learner.capabilities |= RAFT_LEARNER;
        return ClusterConfig{{node(1, 23121, 23221), learner}, ReplicationMode::MIRROR, {},
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 1500},
            raft, {}, 0, testClusterId(23220)};
    }

    int runLearnerPromotionCrashWriter(const char* point, const std::filesystem::path& dir) {
        auto leader = makeRuntime(dir / "n1", learnerCrashConfig(), 1);
        leader->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return leader->runtime->role() == NodeRole::PRIMARY; }, std::chrono::seconds{20}));
        leader->runtime->shipEntry(1, ReplOpType::PUT, bytesOf("promotion"), bytesOf("before"), 0, 1);
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto stats = leader->runtime->raftStats();
            return std::ranges::any_of(stats.peers, [&](const RaftPeerStats& peer) { return peer.nodeId == 2 && peer.matchIndex == stats.lastLogIndex; });
        }));
        setStorageCrashPoint(point);
        leader->runtime->promoteRaftLearner(2);
        return 42;
    }

    void testRaftLearnerPromotionCrashRecovery(const std::filesystem::path& executable) {
        for (const char* point : {"raft.learner.after_joint_replication", "raft.learner.after_joint_commit"}) {
            const auto dir = makeTempDir(point);
            const auto config = learnerCrashConfig();
            auto n2 = makeRuntime(dir / "n2", config, 2);
            n2->runtime->start();
            runStorageCrashChild(executable, "--learner-promotion-crash-writer", point, dir);
            auto n1 = makeRuntime(dir / "n1", config, 1);
            n1->runtime->start();
            std::array<RuntimeHarness*, 2> nodes{n1.get(), n2.get()};
            ensurePreferredLeader(nodes, std::chrono::seconds{30});
            auto* leader = findLeader(nodes);
            AKK_CLUSTER_CHECK(leader != nullptr);
            AKK_CLUSTER_CHECK(waitUntil([&] {
                const auto stats = leader->runtime->raftStats();
                return std::ranges::all_of(stats.peers, [&](const RaftPeerStats& peer) { return peer.matchIndex == stats.lastLogIndex; });
            }));
            leader->runtime->promoteRaftLearner(2);
            AKK_CLUSTER_CHECK(leader->runtime->raftStats().voterCount == 2);
            AKK_CLUSTER_CHECK(leader->runtime->raftStats().learnerCount == 0);
            leader->runtime->shipEntry(2, ReplOpType::PUT, bytesOf("promotion"), bytesOf("after"), 0, leader->nodeId);
            AKK_CLUSTER_CHECK(waitUntil([&] { return n1->hasState(2, "promotion", "after") && n2->hasState(2, "promotion", "after"); }));
            n2->runtime->close();
            n1->runtime->close();
        }
    }

    void testRaftLearnerLifecycle(bool secure = false) {
        const auto dir = makeTempDir(secure ? "raft-learners-secure" : "raft-learners");
        auto raft = onlineRaftMembership();
        raft.membership.allowLearners = true;
        const auto n1Info = node(1, 23101, 23201);
        auto n2Info = node(2, 23102, 23202);
        n2Info.capabilities |= RAFT_LEARNER;
        const auto n3Info = node(3, 23103, 23203);
        const ConsistencyOptions consistency{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 1000};
        const ClusterConfig initial{{n1Info}, ReplicationMode::MIRROR, {}, consistency, raft};
        const ClusterConfig joining{{n1Info, n2Info}, ReplicationMode::MIRROR, {}, consistency, raft,
            {}, 0, initial.clusterId()};
        ClusterConfig::save(dir / "learner.akcc", joining);
        AKK_CLUSTER_CHECK(ClusterConfig::load(dir / "learner.akcc").findById(2)->raftLearner());
        ClusterRuntimeOptions options;
        options.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        options.raftSnapshot.minLogEntries = 1;
        if (secure) {
            for (uint64_t id = 1; id <= 3; ++id) {
                const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
                options.secure.pinnedPeers.push_back({id, identity.publicKey});
            }
        }
        const auto makePeer = [&](uint64_t id, const ClusterConfig& config) {
            auto peerOptions = options;
            peerOptions.secure.identitySeedPath = dir / ("identity-" + std::to_string(id));
            return makeRuntime(dir / ("n" + std::to_string(id)), config, id, peerOptions, secure);
        };
        const auto rejects = [](const auto& operation) {
            bool rejected = false;
            try { operation(); } catch (const std::exception&) { rejected = true; }
            AKK_CLUSTER_CHECK(rejected);
        };
        auto n1 = makePeer(1, initial);
        n1->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return n1->runtime->role() == NodeRole::PRIMARY; }));
        n1->runtime->addRaftLearner(n2Info);
        n1->runtime->addRaftLearner(n3Info);
        AKK_CLUSTER_CHECK(n1->runtime->raftStats().voterCount == 1);
        AKK_CLUSTER_CHECK(n1->runtime->raftStats().learnerCount == 2);
        rejects([&] { n1->runtime->promoteRaftLearner(2); });
        rejects([&] { n1->runtime->addRaftVotingNode(n2Info); });
        rejects([&] { n1->runtime->removeRaftVotingNode(2); });
        rejects([&] { n1->runtime->removeRaftVotingNode(1); });
        for (uint64_t seq = 1; seq <= 4; ++seq) {
            n1->runtime->shipEntry(seq, ReplOpType::PUT, bytesOf("learner"), bytesOf(std::to_string(seq)), 0, 1);
        }
        AKK_CLUSTER_CHECK(n1->runtime->readKey(bytesOf("learner"), 0).status == ReadStatus::FOUND);
        AKK_CLUSTER_CHECK(waitUntil([&] { return n1->runtime->raftStats().snapshotIndex > 0; }));
        // Reopen after compaction so no pre-compaction replication RPC can
        // satisfy catch-up from a still-resident entry batch.
        n1->runtime->close();
        n1 = makePeer(1, initial);
        n1->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return n1->runtime->role() == NodeRole::PRIMARY; }, std::chrono::seconds{20}));

        auto n2 = makePeer(2, joining);
        n2->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return n2->hasState(4, "learner", "4"); }, std::chrono::seconds{20}));
        AKK_CLUSTER_CHECK(n2->snapshotsInstalled.load() != 0);
        AKK_CLUSTER_CHECK(n2->runtime->raftStats().localLearner);
        rejects([&] { n1->runtime->transferRaftLeadership(2); });
        rejects([&] { n2->runtime->shipEntry(5, ReplOpType::PUT, bytesOf("learner"), bytesOf("forbidden"), 0, 2); });
        const auto learnerTerm = n2->runtime->raftStats().currentTerm;
        n1->runtime->close();
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{6500};
        AKK_CLUSTER_CHECK(waitUntil([&] {
            AKK_CLUSTER_CHECK(n2->runtime->role() != NodeRole::PRIMARY);
            AKK_CLUSTER_CHECK(n2->runtime->raftStats().currentTerm == learnerTerm);
            return std::chrono::steady_clock::now() >= deadline;
        }, std::chrono::seconds{10}));
        n2->runtime->close();
        n1 = makePeer(1, initial);
        n2 = makePeer(2, joining);
        AKK_CLUSTER_CHECK(n1->runtime->raftStats().learnerCount == 2);
        AKK_CLUSTER_CHECK(n2->runtime->raftStats().localLearner);
        n1->runtime->start();
        n2->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return n1->runtime->role() == NodeRole::PRIMARY; }, std::chrono::seconds{20}));
        n1->runtime->shipEntry(5, ReplOpType::PUT, bytesOf("learner"), bytesOf("5"), 0, 1);
        AKK_CLUSTER_CHECK(waitUntil([&] { return n2->hasState(5, "learner", "5"); }));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto stats = n1->runtime->raftStats();
            return std::ranges::any_of(stats.peers, [&](const RaftPeerStats& peer) {
                return peer.nodeId == 2 && peer.matchIndex == stats.lastLogIndex;
            });
        }));
        n1->runtime->promoteRaftLearner(2);
        AKK_CLUSTER_CHECK(waitUntil([&] { return !n2->runtime->raftStats().localLearner; }));
        AKK_CLUSTER_CHECK(n1->runtime->raftStats().voterCount == 2);
        AKK_CLUSTER_CHECK(n1->runtime->raftStats().learnerCount == 1);
        n1->runtime->transferRaftLeadership(2);
        AKK_CLUSTER_CHECK(waitUntil([&] { return n2->runtime->role() == NodeRole::PRIMARY; }, std::chrono::seconds{20}));
        n2->runtime->removeRaftLearner(3);
        AKK_CLUSTER_CHECK(n2->runtime->raftStats().learnerCount == 0);
        n2->runtime->shipEntry(6, ReplOpType::PUT, bytesOf("learner"), bytesOf("6"), 0, 2);
        AKK_CLUSTER_CHECK(waitUntil([&] { return n1->hasState(6, "learner", "6"); }));
        AKK_CLUSTER_CHECK(waitUntil([&] { return n1->runtime->raftStats().learnerCount == 0; }));
        n2->runtime->close();
        n1->runtime->close();
        n1 = makePeer(1, initial);
        n2 = makePeer(2, joining);
        AKK_CLUSTER_CHECK(n1->runtime->raftStats().voterCount == 2);
        AKK_CLUSTER_CHECK(n2->runtime->raftStats().voterCount == 2);
        AKK_CLUSTER_CHECK(!n2->runtime->raftStats().localLearner);
        AKK_CLUSTER_CHECK(n1->runtime->raftStats().learnerCount == 0);
        n1->runtime->start();
        n2->runtime->start();
        std::array<RuntimeHarness*, 2> voters{n1.get(), n2.get()};
        ensurePreferredLeader(voters, std::chrono::seconds{20});
        AKK_CLUSTER_CHECK(findLeader(voters)->runtime->readKey(bytesOf("learner"), 0).status == ReadStatus::FOUND);
        n2->runtime->close();
        n1->runtime->close();
    }

    void testRaftLearnersNeverSupplyQuorum() {
        const auto dir = makeTempDir("raft-learner-quorum");
        auto raft = onlineRaftMembership();
        raft.membership.allowLearners = true;
        auto n4Info = node(4, 23114, 23214);
        auto n5Info = node(5, 23115, 23215);
        n4Info.capabilities |= RAFT_LEARNER;
        n5Info.capabilities |= RAFT_LEARNER;
        const ClusterConfig config{{node(1, 23111, 23211), node(2, 23112, 23212), node(3, 23113, 23213), n4Info, n5Info},
            ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutMs = 1000}, raft};
        ClusterRuntimeOptions options;
        options.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        auto n1 = makeRuntime(dir / "n1", config, 1, options);
        auto n2 = makeRuntime(dir / "n2", config, 2, options);
        auto n4 = makeRuntime(dir / "n4", config, 4, options);
        auto n5 = makeRuntime(dir / "n5", config, 5, options);
        for (auto* node : {n1.get(), n2.get(), n4.get(), n5.get()}) { node->runtime->start(); }
        std::array<RuntimeHarness*, 2> voters{n1.get(), n2.get()};
        ensurePreferredLeader(voters, std::chrono::seconds{20});
        auto* leader = findLeader(voters);
        AKK_CLUSTER_CHECK(leader != nullptr);
        leader->runtime->shipEntry(1, ReplOpType::PUT, bytesOf("quorum"), bytesOf("committed"), 0, leader->nodeId);
        AKK_CLUSTER_CHECK(leader->runtime->readKey(bytesOf("quorum"), 0).status == ReadStatus::FOUND);
        AKK_CLUSTER_CHECK(waitUntil([&] { return n4->hasState(1, "quorum", "committed") && n5->hasState(1, "quorum", "committed"); }));
        (leader == n1.get() ? n2.get() : n1.get())->runtime->close();
        bool rejected = false;
        try { leader->runtime->shipEntry(2, ReplOpType::PUT, bytesOf("quorum"), bytesOf("uncommitted"), 0, leader->nodeId); }
        catch (const std::runtime_error&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected);
        AKK_CLUSTER_CHECK(leader->runtime->readKey(bytesOf("quorum"), 0).status == ReadStatus::ERROR_STATUS);
        AKK_CLUSTER_CHECK(n4->lastSeq.load() == 1 && n5->lastSeq.load() == 1);
        for (auto* node : {n1.get(), n2.get(), n4.get(), n5.get()}) { node->runtime->close(); }
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
        constexpr std::array<uint64_t, 2> placement{1, 2};
        const auto key = keyPlacedOn(stripeStorageConfig(), placement, "repair-stats");
        const auto repairOptions = [&](uint64_t nodeId) {
            auto options = stripeStorageOptions(dir, nodeId);
            options.cluster.runtime.stripeAutoRebuild = false;
            options.cluster.runtime.stripeWriteCommitMode = StripeWriteCommitMode::ALL_SHARDS;
            return options;
        };
        auto n1 = akkaradb::engine::AkkEngine::open(repairOptions(1));
        auto n2 = akkaradb::engine::AkkEngine::open(repairOptions(2));
        auto n3 = akkaradb::engine::AkkEngine::open(repairOptions(3));
        std::string lastWriteFailure;
        const bool writeReady = waitUntil([&] {
            try { n1->put(bytesOf(key), bytesOf("repair-value")); return true; }
            catch (const std::runtime_error& error) { lastWriteFailure = error.what(); return false; }
        });
        if (!writeReady) { throw std::runtime_error("STRIPE read-repair setup write did not become ready: " + lastWriteFailure); }
        AKK_CLUSTER_CHECK(n1->stats().stripeReadRepair.attempts == 0);
        n2->close();
        n2.reset();
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto stats = n1->stats();
            const auto peer = std::ranges::find(stats.cluster.peers, uint64_t{2}, &akkaradb::engine::EngineStats::ClusterStats::PeerStats::nodeId);
            return peer != stats.cluster.peers.end() && !peer->connected;
        }));
        const auto value = engineGet(*n1, key);
        AKK_CLUSTER_CHECK(value && textOf(*value) == "repair-value");
        const auto failed = n1->stats().stripeReadRepair;
        if (failed.attempts != 1 || failed.failed != 1 || failed.succeeded != 0 || failed.lastFailureNodeId != 2) {
            throw std::runtime_error(
                "unexpected STRIPE repair counters: attempts=" + std::to_string(failed.attempts) +
                " failed=" + std::to_string(failed.failed) + " succeeded=" + std::to_string(failed.succeeded) +
                " lastFailureNodeId=" + std::to_string(failed.lastFailureNodeId)
            );
        }
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
        n2 = akkaradb::engine::AkkEngine::open(repairOptions(2));
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
        n3->close();
        AKK_CLUSTER_CHECK(n2->stats().stripeReadRepair.attempts == 0);
        n2->close();
        n1->close();
        auto disabledOptions = repairOptions(1);
        disabledOptions.cluster.runtime.stripeReadRepair = false;
        n1 = akkaradb::engine::AkkEngine::open(disabledOptions);
        n2 = akkaradb::engine::AkkEngine::open(repairOptions(2));
        n3 = akkaradb::engine::AkkEngine::open(repairOptions(3));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            try {
                const auto withoutRepair = engineGet(*n1, key);
                return withoutRepair && textOf(*withoutRepair) == "repair-value";
            }
            catch (const std::runtime_error&) { return false; }
        }));
        AKK_CLUSTER_CHECK(n1->stats().stripeReadRepair.attempts == 0);
        n3->close();
        n2->close();
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

        const AckPolicy ack{.mode = AckPolicyMode::ALL_TARGETS, .stage = AckStage::DURABLE};
        const ConsistencyOptions consistency{
            .mode = ConsistencyMode::PRIMARY_ACK,
            .writeConsistency = WriteConsistency::AVAILABLE_REPLICAS,
            .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE,
        };
        ReplicationServer::Snapshot snapshot;
        snapshot.seq = 8;
        auto snapshotEntries = std::make_shared<std::vector<ReplSnapshotEntry>>();
        for (uint8_t i = 0; i < 8; ++i) {
            snapshotEntries->push_back(ReplSnapshotEntry{
                .key = std::vector<uint8_t>{'k', i},
                .value = std::vector<uint8_t>(256, static_cast<uint8_t>(0x50 + i)),
            });
        }
        snapshot.forEachEntry = [snapshotEntries](const ReplicationServer::Snapshot::EntryVisitor& visitor) {
            for (const auto& entry : *snapshotEntries) {
                if (!visitor.beginEntry(entry.key, entry.value.size(), snapshotValueCrc(entry.value))) { return false; }
                for (size_t offset = 0; offset < entry.value.size();) {
                    const size_t count = std::min<size_t>(31, entry.value.size() - offset);
                    if (!visitor.appendValueChunk(offset, std::span<const uint8_t>{entry.value}.subspan(offset, count))) { return false; }
                    offset += count;
                }
                if (!visitor.finishEntry()) { return false; }
            }
            return true;
        };

        auto server = ReplicationServer::create(
            port,
            1,
            [] { return uint64_t{8}; },
            ack,
            consistency,
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
                AKK_CLUSTER_CHECK(count == 0);
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
            [&](uint64_t seq, uint64_t count) {
                AKK_CLUSTER_CHECK(count == 8);
                lastSeq.store(seq);
            }
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
        snapshot.forEachEntry = [snapshotEntry](const ReplicationServer::Snapshot::EntryVisitor& visitor) {
            return visitor.beginEntry(snapshotEntry->key, snapshotEntry->value.size(), snapshotValueCrc(snapshotEntry->value)) &&
                visitor.appendValueChunk(0, snapshotEntry->value) && visitor.finishEntry();
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
            const ClusterConfig raftMirror{
                {
                    node(1, 20375, 20475),
                    node(2, 20376, 20476),
                    node(3, 20377, 20477),
                },
                ReplicationMode::MIRROR,
                AckPolicy{},
                ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM},
            };
            ClusterRuntimeOptions runtime;
            runtime.transportMode = TransportMode::PLAIN;
            runtime.clusterGroupId = 9102;
            runtime.raftMaxReceiveMemoryBytes = 0;

            bool rejected = false;
            try { raftMirror.validateRuntime(1, runtime); }
            catch (const std::invalid_argument&) { rejected = true; }
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
        writeTextFile(dir / "n1" / "cluster.membership", "not a valid AKCG1 state\n");

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
        writeTextFile(membershipPath, "not a valid AKCG1 state\n");

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

    void testMirrorQuorumFencing(bool secure = false) {
        const auto dir = makeTempDir(secure ? "mirror-fencing-secure" : "mirror-fencing");
        const std::vector<NodeInfo> nodes{node(1, 23301, 23311), node(2, 23302, 23312), node(3, 23303, 23313)};
        const std::vector<NodeInfo> authorities{node(1, 23401, 23411), node(2, 23402, 23412), node(3, 23403, 23413)};
        const ConsistencyOptions consistency{.mode = ConsistencyMode::ASYNC, .ackTimeoutMs = 1000};
        const ClusterConfig config{nodes, ReplicationMode::MIRROR, {}, consistency, {}, {}, 1};
        ClusterRuntimeOptions options;
        options.clusterGroupId = 23300;
        options.mirrorFencing.mode = MirrorFencingMode::QUORUM_FENCED;
        options.mirrorFencing.authorityNodes = authorities;
        options.raftSnapshot.minLogEntries = 1;
        if (secure) {
            for (uint64_t id = 1; id <= 3; ++id) {
                const auto identity = akkaradb::crypto::IdentityStore{dir / ("identity-" + std::to_string(id))}.loadOrCreate();
                options.secure.pinnedPeers.push_back({id, identity.publicKey});
            }
        }
        const auto makePeer = [&](uint64_t id) {
            auto runtime = options;
            runtime.startupRole = id == 1 ? NodeStartupRole::PRIMARY : NodeStartupRole::REPLICA;
            runtime.secure.identitySeedPath = dir / ("identity-" + std::to_string(id));
            return makeRuntime(dir / ("n" + std::to_string(id)), config, id, runtime, secure);
        };
        auto n1 = makePeer(1), n2 = makePeer(2), n3 = makePeer(3);
        n1->runtime->start(); n2->runtime->start(); n3->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return n1->runtime->raftStats().mirrorAuthorityLeaderNodeId == 1; }, std::chrono::seconds{30}));
        const auto write = [&](uint64_t seq, const char* value) {
            n1->runtime->executePrimaryWrite([&] {
                n1->setState(seq, bytesOf("authority-key"), bytesOf(value));
                n1->runtime->shipEntry(seq, ReplOpType::PUT, bytesOf("authority-key"), bytesOf(value), 0, 1);
            });
        };
        write(1, "first");
        AKK_CLUSTER_CHECK(!n1->runtime->raftStats().mirrorWritePending);
        AKK_CLUSTER_CHECK(n1->runtime->raftStats().mirrorAuthorityPrimaryNodeId == 1);
        AKK_CLUSTER_CHECK(waitUntil([&] { return n2->hasState(1, "authority-key", "first") && n3->hasState(1, "authority-key", "first"); }));
        n2->runtime->close();
        write(2, "async-data-with-one-replica-offline");
        AKK_CLUSTER_CHECK(n2->lastSeq.load() == 1);
        AKK_CLUSTER_CHECK(waitUntil([&] { return n3->lastSeq.load() == 2; }));
        n3->runtime->close();
        bool invoked = false, rejected = false;
        try { n1->runtime->executePrimaryWrite([&] { invoked = true; }); }
        catch (const std::runtime_error&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected && !invoked && n1->lastSeq.load() == 2);
        n1->runtime->close();
        n1.reset(); n2.reset(); n3.reset();
        n1 = makePeer(1); n2 = makePeer(2); n3 = makePeer(3);
        n1->runtime->start(); n2->runtime->start(); n3->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return n1->runtime->raftStats().mirrorAuthorityLeaderNodeId == 1; }, std::chrono::seconds{30}));
        write(3, "restart-after-control-snapshot");
        AKK_CLUSTER_CHECK(waitUntil([&] { return n2->hasState(3, "authority-key", "restart-after-control-snapshot"); }));
        bool interrupted = false;
        try { n1->runtime->executePrimaryWrite([] { throw std::runtime_error("interrupted mutation"); }); }
        catch (const std::runtime_error&) { interrupted = true; }
        AKK_CLUSTER_CHECK(interrupted && n1->runtime->raftStats().mirrorWritePending);
        n3->runtime->close(); n2->runtime->close(); n1->runtime->close();
        n1.reset(); n2.reset(); n3.reset();
        n1 = makePeer(1); n2 = makePeer(2); n3 = makePeer(3);
        n1->runtime->start(); n2->runtime->start(); n3->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return n1->runtime->raftStats().mirrorAuthorityLeaderNodeId == 1; }, std::chrono::seconds{30}));
        invoked = false; rejected = false;
        try { n1->runtime->executePrimaryWrite([&] { invoked = true; }); }
        catch (const std::runtime_error&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected && !invoked && n1->runtime->raftStats().mirrorWritePending);
        n3->runtime->close(); n2->runtime->close(); n1->runtime->close();
        auto downgraded = options;
        downgraded.mirrorFencing = {};
        bool downgradeRejected = false;
        try { (void)makeRuntime(dir / "n1", config, 1, downgraded, secure); }
        catch (const std::runtime_error&) { downgradeRejected = true; }
        AKK_CLUSTER_CHECK(downgradeRejected);
        for (const auto& file : std::filesystem::recursive_directory_iterator(dir / "n1" / "mirror-authority")) {
            if (!file.is_regular_file()) { continue; }
            std::ifstream in(file.path(), std::ios::binary);
            const std::string image((std::istreambuf_iterator<char>(in)), {});
            AKK_CLUSTER_CHECK(image.find("async-data-with-one-replica-offline") == std::string::npos);
        }
    }

    void testMirrorFencingEngineApi() {
        const auto dir = makeTempDir("mirror-fencing-engine");
        const ClusterConfig config{{node(1, 23501, 23511), node(2, 23502, 23512), node(3, 23503, 23513, COORDINATOR_ELIGIBLE)},
            ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::ASYNC, .ackTimeoutMs = 1000}, {}, {}, 1};
        const auto makeOptions = [&](uint64_t id) {
            akkaradb::engine::AkkEngineOptions options;
            options.paths.dataDir = dir / ("n" + std::to_string(id));
            options.components.clusterEnabled = true;
            options.components.blobEnabled = false;
            options.components.versionLogEnabled = true;
            options.cluster.config = config;
            auto& runtime = options.cluster.runtime;
            runtime.transportMode = TransportMode::PLAIN;
            runtime.startupRole = id == 1 ? NodeStartupRole::PRIMARY : NodeStartupRole::REPLICA;
            runtime.clusterGroupId = 23500;
            runtime.mirrorFencing.mode = MirrorFencingMode::QUORUM_FENCED;
            runtime.mirrorFencing.authorityNodes = {node(1, 23601, 23611), node(2, 23602, 23612), node(3, 23603, 23613)};
            runtime.routingMode = id == 2 ? ClusterRoutingMode::FORWARD : ClusterRoutingMode::REDIRECT;
            runtime.readMode = id == 2 ? ClusterReadMode::OWNER_LINEARIZABLE : ClusterReadMode::LOCAL_STALE_OK;
            writeNodeIdFile(options.paths.dataDir / "node.id", id);
            return options;
        };
        auto volatileOptions = makeOptions(1);
        volatileOptions.components.walEnabled = false;
        volatileOptions.runtime.writeDurability = akkaradb::engine::AkkEngineOptions::WriteDurabilityMode::MEMORY;
        bool volatileRejected = false;
        try { (void)akkaradb::engine::AkkEngine::open(volatileOptions); }
        catch (const std::invalid_argument&) { volatileRejected = true; }
        AKK_CLUSTER_CHECK(volatileRejected);
        auto n1 = akkaradb::engine::AkkEngine::open(makeOptions(1));
        auto n2 = akkaradb::engine::AkkEngine::open(makeOptions(2));
        auto n3 = akkaradb::engine::AkkEngine::open(makeOptions(3));
        AKK_CLUSTER_CHECK(waitUntil([&] { return n1->stats().cluster.mirrorAuthorityLeaderNodeId == 1; }, std::chrono::seconds{30}));
        n1->put(bytesOf("fenced-api"), bytesOf("one"));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto value = engineGet(*n2, "fenced-api"); return value && textOf(*value) == "one";
        }));
        n2->put(bytesOf("forwarded-fenced"), bytesOf("quorum-authority-async-data"));
        AKK_CLUSTER_CHECK(textOf(*n1->get(bytesOf("forwarded-fenced"))) == "quorum-authority-async-data" && !n1->stats().cluster.mirrorWritePending);
        const std::array<std::string, 4> batchStorage{"batch-one", "one", "batch-two", "two"};
        const std::array<akkaradb::engine::detail::BulkPutEntry, 2> batch{
            akkaradb::engine::detail::BulkPutEntry{bytesOf(batchStorage[0]), bytesOf(batchStorage[1])},
            akkaradb::engine::detail::BulkPutEntry{bytesOf(batchStorage[2]), bytesOf(batchStorage[3])}};
        akkaradb::engine::detail::ProtocolBulkWriter::put(*n1, batch);
        n1->remove(bytesOf("batch-one"));
        AKK_CLUSTER_CHECK(waitUntil([&] { return engineGet(*n2, "batch-two").has_value() && !engineGet(*n2, "batch-one").has_value(); }));
        AKK_CLUSTER_CHECK(n2->count() == 3);
        const auto history = akk_test::collectHistory(n2->history(bytesOf("fenced-api")));
        AKK_CLUSTER_CHECK(history.size() == 1 && textOf(history.front().value) == "one");
        AKK_CLUSTER_CHECK(textOf(*n2->getAt(bytesOf("fenced-api"), history.front().seq)) == "one");
        AKK_CLUSTER_CHECK(!engineGet(*n3, "fenced-api").has_value()); // authority-only witness
        n2->close();
        n1->put(bytesOf("fenced-api"), bytesOf("async"));
        AKK_CLUSTER_CHECK(!n1->stats().cluster.mirrorWritePending);
        n3->close();
        bool rejected = false;
        try { n1->put(bytesOf("denied"), bytesOf("must-not-apply")); } catch (const std::runtime_error&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected && !engineGet(*n1, "denied").has_value());
        n1->close();
    }

    void testMirrorFencingValidation() {
        static_assert(ReplFrameHeader::MAGIC == 0x31524B41); // current pre-release AKR1 protocol
        for (const auto mode : {MirrorFencingMode::STATIC, MirrorFencingMode::QUORUM_FENCED, MirrorFencingMode::EXTERNAL_FENCED}) {
            DecodedFrame frame;
            AKK_CLUSTER_CHECK(decodeFrame(encodeClientHello(ClientHello{.nodeId = 2, .mirrorFencingMode = mode, .forceSnapshot = true}), frame));
            ClientHello client;
            AKK_CLUSTER_CHECK(decodeClientHello(frame.payload, client) && client.mirrorFencingMode == mode && client.forceSnapshot);
            frame.payload[17] = 6;
            AKK_CLUSTER_CHECK(!decodeClientHello(frame.payload, client));
            AKK_CLUSTER_CHECK(decodeFrame(encodeServerHello(ServerHello{.nodeId = 1, .mirrorFencingMode = mode}), frame));
            ServerHello server;
            AKK_CLUSTER_CHECK(decodeServerHello(frame.payload, server) && server.mirrorFencingMode == mode);
            frame.payload[17] = 3;
            AKK_CLUSTER_CHECK(!decodeServerHello(frame.payload, server));
        }
        const auto dir = makeTempDir("mirror-fencing-validation");
        const ClusterConfig config{{node(1, 23331, 23341), node(2, 23332, 23342)}, ReplicationMode::MIRROR, {}, {}, {}, {}, 2};
        ClusterRuntimeOptions options;
        options.startupRole = NodeStartupRole::PRIMARY;
        options.mirrorPromotion = MirrorPromotionOptions{true, 1, 1, 0};
        bool rejected = false;
        try { (void)makeRuntime(dir / "static", config, 2, options); } catch (const std::invalid_argument&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected);
        options.mirrorPromotion = {};
        options.mirrorFencing.mode = MirrorFencingMode::EXTERNAL_FENCED;
        rejected = false;
        try { (void)makeRuntime(dir / "external", config, 2, options); } catch (const std::invalid_argument&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected);
        options.mirrorFencing.mode = MirrorFencingMode::QUORUM_FENCED;
        rejected = false;
        try { (void)makeRuntime(dir / "quorum", config, 2, options); } catch (const std::invalid_argument&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected);
    }

    class TestMirrorFencer final : public IMirrorFencingProvider {
    public:
        uint64_t primary = 1, epoch = 1;
        bool failFence = false;
        unsigned fenced = 0;
        void fence(const MirrorFenceRequest& request) override {
            if (failFence) { throw std::runtime_error("test fencing failed"); }
            AKK_CLUSTER_CHECK(request.groupId != 0 && request.previousPrimaryNodeId == 1 && request.previousGroupEpoch == 1);
            primary = request.candidateNodeId; epoch = request.previousGroupEpoch + 1; ++fenced;
        }
        void validatePrimary(const ClusterId&, uint64_t group, uint64_t node, uint64_t generation) override {
            if (group == 0 || node != primary || generation != epoch) { throw std::runtime_error("test: fenced Primary"); }
        }
    };

    void testMirrorQuorumPromotion(bool unresolved) {
        const auto dir = makeTempDir(unresolved ? "mirror-quorum-pending-promotion" : "mirror-quorum-promotion");
        const std::vector<NodeInfo> nodes{node(1, 23701, 23711), node(2, 23702, 23712), node(3, 23703, 23713)};
        const std::vector<NodeInfo> authorities{node(1, 23801, 23811), node(2, 23802, 23812), node(3, 23803, 23813)};
        const ConsistencyOptions consistency{.mode = ConsistencyMode::ASYNC, .ackTimeoutMs = 1000};
        const ClusterConfig original{nodes, ReplicationMode::MIRROR, {}, consistency, {}, {}, 1};
        ClusterRuntimeOptions options;
        options.clusterGroupId = 23700;
        options.mirrorFencing.mode = MirrorFencingMode::QUORUM_FENCED;
        options.mirrorFencing.authorityNodes = authorities;
        const auto makePeer = [&](uint64_t id, const ClusterConfig& config, bool promoting,
                                  std::shared_ptr<IMirrorFencingProvider> external = {}) {
            auto runtime = options;
            runtime.startupRole = id == config.primaryNodeId() ? NodeStartupRole::PRIMARY : NodeStartupRole::REPLICA;
            if (external) {
                runtime.mirrorRecovery.mode = MirrorRecoveryMode::MANUAL;
                runtime.mirrorRecovery.provider = std::make_shared<MirrorFencingRecoveryProvider>(std::move(external));
            }
            if (promoting) { runtime.mirrorPromotion = MirrorPromotionOptions{true, 1, 1, 1}; }
            return makeRuntime(dir / ("n" + std::to_string(id)), config, id, runtime);
        };
        auto n1 = makePeer(1, original, false), n2 = makePeer(2, original, false), n3 = makePeer(3, original, false);
        n1->runtime->start(); n2->runtime->start(); n3->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return n1->runtime->raftStats().mirrorAuthorityLeaderNodeId == 1; }, std::chrono::seconds{30}));
        n1->runtime->executePrimaryWrite([&] {
            n1->setState(1, bytesOf("promotion-key"), bytesOf("before"));
            n1->runtime->shipEntry(1, ReplOpType::PUT, bytesOf("promotion-key"), bytesOf("before"), 0, 1);
        });
        AKK_CLUSTER_CHECK(waitUntil([&] { return n2->hasState(1, "promotion-key", "before") && n3->hasState(1, "promotion-key", "before"); }));
        if (unresolved) {
            bool failed = false;
            try { n1->runtime->executePrimaryWrite([] { throw std::runtime_error("write interrupted after grant"); }); }
            catch (const std::runtime_error&) { failed = true; }
            AKK_CLUSTER_CHECK(failed && n1->runtime->raftStats().mirrorWritePending);
            AKK_CLUSTER_CHECK(waitUntil([&] { return n2->runtime->raftStats().mirrorWritePending && n3->runtime->raftStats().mirrorWritePending; }));
        }
        n3->runtime->close(); n2->runtime->close(); n1->runtime->close();
        n1.reset(); n2.reset(); n3.reset();
        const ClusterConfig promoted{nodes, ReplicationMode::MIRROR, {}, consistency, {}, {}, 2, original.clusterId()};
        n3 = makePeer(3, promoted, true); n3->runtime->start();
        n2 = makePeer(2, promoted, true);
        if (unresolved) {
            bool rejected = false;
            try { n2->runtime->start(); } catch (const std::runtime_error&) { rejected = true; }
            AKK_CLUSTER_CHECK(rejected);
            AKK_CLUSTER_CHECK(readGroupState(dir / "n2" / "cluster.membership").primaryNodeId == 1);
            n2->runtime->close(); n2.reset();
            const auto fencer = std::make_shared<TestMirrorFencer>();
            n2 = makePeer(2, promoted, true, fencer); n2->runtime->start();
            AKK_CLUSTER_CHECK(fencer->fenced == 1);
        }
        else { n2->runtime->start(); }
        AKK_CLUSTER_CHECK(n2->runtime->raftStats().mirrorAuthorityPrimaryNodeId == 2);
        AKK_CLUSTER_CHECK(!n2->runtime->raftStats().mirrorWritePending);
        AKK_CLUSTER_CHECK(readGroupState(dir / "n2" / "cluster.membership").groupEpoch == 2);
        AKK_CLUSTER_CHECK(waitUntil([&] { return n3->snapshotsInstalled.load() > 0; }));
        n2->runtime->executePrimaryWrite([&] {
            n2->setState(2, bytesOf("promotion-key"), bytesOf("after"));
            n2->runtime->shipEntry(2, ReplOpType::PUT, bytesOf("promotion-key"), bytesOf("after"), 0, 2);
        });
        AKK_CLUSTER_CHECK(waitUntil([&] { return n3->hasState(2, "promotion-key", "after"); }));
        AKK_CLUSTER_CHECK(waitUntil([&] { return !std::filesystem::exists(dir / "n3" / "cluster.membership.mirror-resync"); }));
        n3->runtime->close();
        n3->setState(2, bytesOf("promotion-key"), bytesOf("unreconciled"));
        std::filesystem::copy_file(dir / "n3" / "cluster.membership", dir / "n3" / "cluster.membership.mirror-resync");
        n3.reset(); n3 = makePeer(3, promoted, false); n3->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return n3->hasState(2, "promotion-key", "after") && n3->snapshotsInstalled.load() > 0; }));
        // A stale process/config may restart, but cannot get a new write permit.
        n1 = makePeer(1, original, false); n1->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return n1->runtime->raftStats().mirrorAuthorityPrimaryNodeId == 2; }));
        bool invoked = false, rejected = false;
        try { n1->runtime->executePrimaryWrite([&] { invoked = true; }); }
        catch (const std::runtime_error&) { rejected = true; }
        AKK_CLUSTER_CHECK(rejected && !invoked && n1->lastSeq.load() == 1);
        n1->runtime->close(); n2->runtime->close();
        // Replay a previous-generation frame through an otherwise valid successor connection.
        ClusterRuntimeOptions replayOptions;
        replayOptions.transportMode = TransportMode::PLAIN;
        replayOptions.clusterGroupId = 23700; replayOptions.clusterGroupEpoch = 2;
        replayOptions.mirrorFencing.mode = MirrorFencingMode::QUORUM_FENCED;
        replayOptions.replicationClusterId = promoted.clusterId();
        replayOptions.replicationConfigFingerprint = promoted.replicationFingerprint();
        auto replay = ReplicationServer::create(23712, 2, [] { return uint64_t{2}; }, {}, consistency, 1, {3}, replayOptions);
        replay->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return replay->replicaCount() == 1; }));
        std::vector<uint8_t> staleKey{'A', 'K', 'M', 'D', '1'};
        for (size_t i = 0; i < 8; ++i) { staleKey.push_back(static_cast<uint8_t>(uint64_t{1} >> (i * 8))); }
        const std::string publicKey = "promotion-key";
        staleKey.insert(staleKey.end(), publicKey.begin(), publicKey.end());
        replay->shipEntry(3, ReplOpType::PUT, staleKey, bytesOf("obsolete"), 0, 2);
        AKK_CLUSTER_CHECK(waitUntil([&] { return replay->replicaCount() == 0; }));
        AKK_CLUSTER_CHECK(n3->hasState(2, "promotion-key", "after"));
        replay->close(); n3->runtime->close();
    }

    void testMirrorQuorumCompletionRecovery(bool restartPrimary = true) {
        const auto dir = makeTempDir(restartPrimary ? "mirror-quorum-completion-recovery" : "mirror-quorum-live-completion-recovery");
        const ClusterConfig config{{node(1, 23901, 23911), node(2, 23902, 23912), node(3, 23903, 23913)},
            ReplicationMode::MIRROR, {}, ConsistencyOptions{.mode = ConsistencyMode::ASYNC, .ackTimeoutMs = 500}, {}, {}, 1};
        const auto makePeer = [&](uint64_t id) {
            ClusterRuntimeOptions options;
            options.clusterGroupId = 23900;
            options.startupRole = id == 1 ? NodeStartupRole::PRIMARY : NodeStartupRole::REPLICA;
            options.mirrorFencing.mode = MirrorFencingMode::QUORUM_FENCED;
            options.mirrorFencing.authorityNodes = {node(1, 24001, 24011), node(2, 24002, 24012), node(3, 24003, 24013)};
            return makeRuntime(dir / ("n" + std::to_string(id)), config, id, options);
        };
        auto n1 = makePeer(1), n2 = makePeer(2), n3 = makePeer(3);
        n1->runtime->start(); n2->runtime->start(); n3->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] { return n1->runtime->raftStats().mirrorAuthorityLeaderNodeId == 1; }, std::chrono::seconds{30}));
        bool failed = false;
        uint64_t invocations = 0;
        try {
            n1->runtime->executePrimaryWrite([&] {
                ++invocations;
                n2->runtime->close(); n3->runtime->close(); // grant committed, release cannot reach quorum
                std::this_thread::sleep_for(std::chrono::milliseconds{1200}); // pause longer than the authority timeout
                AKK_CLUSTER_CHECK(n1->runtime->raftStats().mirrorWritePending);
                n1->setState(1, bytesOf("completed-key"), bytesOf("durable"));
            });
        }
        catch (const std::runtime_error&) { failed = true; }
        AKK_CLUSTER_CHECK(failed && n1->hasState(1, "completed-key", "durable"));
        AKK_CLUSTER_CHECK(std::filesystem::exists(dir / "n1" / "mirror-authority" / "completed"));
        AKK_CLUSTER_CHECK(n1->runtime->raftStats().mirrorWritePending);
        n2.reset(); n3.reset();
        if (restartPrimary) {
            n1->runtime->close(); n1.reset();
            n1 = makePeer(1); n1->runtime->start();
        }
        n2 = makePeer(2); n3 = makePeer(3);
        n2->runtime->start(); n3->runtime->start();
        // Stats are passive: no public write/read may trigger this recovery.
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto stats = n1->runtime->raftStats();
            return stats.mirrorAuthorityPrimaryNodeId == 1 && stats.mirrorAuthorityEpoch == 1 &&
                !stats.mirrorWritePending && !n2->runtime->raftStats().mirrorWritePending && !n3->runtime->raftStats().mirrorWritePending;
        }, std::chrono::seconds{30}));
        AKK_CLUSTER_CHECK(invocations == 1 && n1->hasState(1, "completed-key", "durable"));
        bool invoked = false;
        n1->runtime->executePrimaryWrite([&] { invoked = true; });
        AKK_CLUSTER_CHECK(invoked && !n1->runtime->raftStats().mirrorWritePending);
        AKK_CLUSTER_CHECK(n1->hasState(1, "completed-key", "durable"));
        // The previous receipt must not certify a later interrupted operation.
        failed = false;
        try { n1->runtime->executePrimaryWrite([] { throw std::runtime_error("no completion receipt for this grant"); }); }
        catch (const std::runtime_error&) { failed = true; }
        AKK_CLUSTER_CHECK(failed && n1->runtime->raftStats().mirrorWritePending);
        invoked = false; failed = false;
        try { n1->runtime->executePrimaryWrite([&] { invoked = true; }); }
        catch (const std::runtime_error&) { failed = true; }
        AKK_CLUSTER_CHECK(failed && !invoked && n1->runtime->raftStats().mirrorWritePending);
        n3->runtime->close(); n2->runtime->close(); n1->runtime->close();
    }

    void testNonRaftMirrorOfflinePromotion() {
        const auto dir = makeTempDir("non-raft-mirror-promotion");
        const std::vector<NodeInfo> nodes{
            node(1, 20581, 20591),
            node(2, 20582, 20592),
        };
        const AckPolicy ack{};
        const ConsistencyOptions consistency{
            .mode = ConsistencyMode::PRIMARY_ACK,
            .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE,
            .ackTimeoutMs = 2000,
        };
        const ClusterConfig original{nodes, ReplicationMode::MIRROR, ack, consistency, {}, {}, 1};
        const auto fencer = std::make_shared<TestMirrorFencer>();
        const MirrorFencingOptions fencing{.mode = MirrorFencingMode::EXTERNAL_FENCED, .authorityNodes = {}, .external = fencer};

        ClusterRuntimeOptions oldPrimaryOptions;
        oldPrimaryOptions.startupRole = NodeStartupRole::PRIMARY;
        oldPrimaryOptions.mirrorFencing = fencing;
        ClusterRuntimeOptions oldReplicaOptions;
        oldReplicaOptions.startupRole = NodeStartupRole::REPLICA;
        oldReplicaOptions.mirrorFencing = fencing;
        auto oldPrimary = makeRuntime(dir / "n1", original, 1, oldPrimaryOptions);
        auto candidate = makeRuntime(dir / "n2", original, 2, oldReplicaOptions);
        oldPrimary->runtime->start();
        candidate->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto stats = oldPrimary->runtime->raftStats();
            return std::ranges::any_of(stats.peers, [](const auto& peer) { return peer.nodeId == 2 && peer.connected; });
        }));

        oldPrimary->setState(7, bytesOf("mirror-key"), bytesOf("before-promotion"));
        candidate->setState(7, bytesOf("mirror-key"), bytesOf("before-promotion"));
        const auto originalGroup = readGroupState(dir / "n1" / "cluster.membership");
        AKK_CLUSTER_CHECK(waitUntil([&] {
            std::error_code ec;
            return std::filesystem::file_size(dir / "n2" / "cluster.membership", ec) == 33 && !ec;
        }));
        AKK_CLUSTER_CHECK(readGroupState(dir / "n2" / "cluster.membership").groupId == originalGroup.groupId);
        candidate->runtime->close();
        oldPrimary->runtime->close();
        candidate.reset();
        oldPrimary.reset();

        const ClusterConfig promoted{nodes, ReplicationMode::MIRROR, ack, consistency, {}, {}, 2, original.clusterId()};
        ClusterRuntimeOptions unapproved;
        unapproved.startupRole = NodeStartupRole::PRIMARY;
        unapproved.mirrorFencing = fencing;
        auto rejected = makeRuntime(dir / "n2", promoted, 2, unapproved);
        bool rejectedWithoutAuthorization = false;
        try { rejected->runtime->start(); }
        catch (const std::runtime_error&) { rejectedWithoutAuthorization = true; }
        AKK_CLUSTER_CHECK(rejectedWithoutAuthorization);
        rejected->runtime->close();
        rejected.reset();
        AKK_CLUSTER_CHECK(readGroupState(dir / "n2" / "cluster.membership").primaryNodeId == 1);

        ClusterRuntimeOptions promotion;
        promotion.startupRole = NodeStartupRole::PRIMARY;
        promotion.mirrorFencing = fencing;
        promotion.mirrorPromotion = MirrorPromotionOptions{
            .enabled = true,
            .previousPrimaryNodeId = 1,
            .previousGroupEpoch = originalGroup.groupEpoch,
            .expectedDurableSeq = 6,
        };
        auto staleCandidate = makeRuntime(dir / "n2", promoted, 2, promotion);
        bool rejectedStaleCandidate = false;
        try { staleCandidate->runtime->start(); }
        catch (const std::runtime_error&) { rejectedStaleCandidate = true; }
        AKK_CLUSTER_CHECK(rejectedStaleCandidate);
        staleCandidate->runtime->close();
        staleCandidate.reset();
        AKK_CLUSTER_CHECK(readGroupState(dir / "n2" / "cluster.membership").groupEpoch == originalGroup.groupEpoch);

        promotion.mirrorPromotion.expectedDurableSeq = 7;
        promotion.clusterGroupId = originalGroup.groupId == UINT64_MAX
            ? originalGroup.groupId - 1
            : originalGroup.groupId + 1;
        auto wrongGroupCandidate = makeRuntime(dir / "n2", promoted, 2, promotion);
        bool rejectedWrongGroup = false;
        try { wrongGroupCandidate->runtime->start(); }
        catch (const std::runtime_error&) { rejectedWrongGroup = true; }
        AKK_CLUSTER_CHECK(rejectedWrongGroup);
        wrongGroupCandidate->runtime->close();
        wrongGroupCandidate.reset();
        AKK_CLUSTER_CHECK(readGroupState(dir / "n2" / "cluster.membership").groupEpoch == originalGroup.groupEpoch);

        promotion.clusterGroupId = 0;
        fencer->failFence = true;
        auto denied = makeRuntime(dir / "n2", promoted, 2, promotion);
        bool deniedFence = false;
        try { denied->runtime->start(); } catch (const std::runtime_error&) { deniedFence = true; }
        AKK_CLUSTER_CHECK(deniedFence);
        AKK_CLUSTER_CHECK(readGroupState(dir / "n2" / "cluster.membership").groupEpoch == originalGroup.groupEpoch);
        denied.reset();
        fencer->failFence = false;
        auto newPrimary = makeRuntime(dir / "n2", promoted, 2, promotion);
        newPrimary->runtime->start();
        const auto promotedGroup = readGroupState(dir / "n2" / "cluster.membership");
        AKK_CLUSTER_CHECK(promotedGroup.groupId == originalGroup.groupId);
        AKK_CLUSTER_CHECK(promotedGroup.primaryNodeId == 2);
        AKK_CLUSTER_CHECK(promotedGroup.groupEpoch == originalGroup.groupEpoch + 1);

        ClusterRuntimeOptions demoted;
        demoted.startupRole = NodeStartupRole::REPLICA;
        demoted.mirrorFencing = fencing;
        demoted.readMode = ClusterReadMode::OWNER_LINEARIZABLE;
        demoted.mirrorPromotion = promotion.mirrorPromotion;
        auto oldAsReplica = makeRuntime(dir / "n1", promoted, 1, demoted);
        oldAsReplica->runtime->start();
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto stats = newPrimary->runtime->raftStats();
            return std::ranges::any_of(stats.peers, [](const auto& peer) { return peer.nodeId == 1 && peer.connected; });
        }));
        AKK_CLUSTER_CHECK(waitUntil([&] {
            const auto path = dir / "n1" / "cluster.membership";
            std::error_code ec;
            if (!std::filesystem::exists(path, ec) || ec || std::filesystem::file_size(path, ec) != 33 || ec) { return false; }
            try {
                const auto state = readGroupState(path);
                return state.primaryNodeId == 2 && state.groupEpoch == promotedGroup.groupEpoch;
            }
            catch (const std::runtime_error&) { return false; }
        }));
        const auto demotedGroup = readGroupState(dir / "n1" / "cluster.membership");
        AKK_CLUSTER_CHECK(demotedGroup.primaryNodeId == 2);
        AKK_CLUSTER_CHECK(demotedGroup.groupEpoch == promotedGroup.groupEpoch);

        newPrimary->setState(8, bytesOf("mirror-key"), bytesOf("after-promotion"));
        const auto routedRead = oldAsReplica->runtime->readKey(bytesOf("mirror-key"), 8);
        AKK_CLUSTER_CHECK(routedRead.status == ReadStatus::FOUND);
        AKK_CLUSTER_CHECK(textOf(routedRead.value) == "after-promotion");

        oldAsReplica->runtime->close();
        newPrimary->runtime->close();
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
            ConsistencyOptions{.mode = ConsistencyMode::RAFT_QUORUM, .ackTimeoutAction = AckTimeoutAction::FAIL_WRITE, .ackTimeoutMs = 3000},
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
        AKK_CLUSTER_CHECK(std::filesystem::exists(options.paths.dataDir / "cluster-raft.log"));
        AKK_CLUSTER_CHECK(!std::filesystem::exists(options.paths.dataDir / "raft.log"));
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
        options.cluster.runtime.routingMode = ClusterRoutingMode::FORWARD;
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
        AKK_CLUSTER_CHECK(engine->stats().putsTotal == 1);

        AKK_CLUSTER_CHECK(engine->count() == 1);
        akkaradb::core::BufferArena arena;
        size_t records = 0;
        for (const auto& record : engine->scan(arena)) {
            AKK_CLUSTER_CHECK(textOf(record.key) == key && textOf(record.value) == value); ++records;
        }
        AKK_CLUSTER_CHECK(records == 1);
        const auto history = akk_test::collectHistory(engine->history(bytesOf(key)));
        AKK_CLUSTER_CHECK(history.size() == 1 && textOf(history.front().value) == value);
        AKK_CLUSTER_CHECK(textOf(*engine->getAt(bytesOf(key), history.front().seq)) == value);
        engine->close();
    }

    #include "detail/cluster_queries_test.inc"
#include "detail/cluster_authority_contract_test.inc"
#include "detail/cluster_query_resource_test.inc"
#include "detail/cluster_query_delivery_test.inc"
#include "detail/cluster_partition_placement_test.inc"
#include "detail/cluster_partition_raft_test.inc"
#include "detail/cluster_placement_options_test.inc"
#include "detail/cluster_log_retention_test.inc"
#include "detail/cluster_mirror_recovery_test.inc"
#include "detail/cluster_parallelism_test.inc"

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
            options.cluster.runtime.raftBlobChunkSizeBytes = 64u * 1024u;
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
        const std::vector<uint8_t> value = deterministicBytes(1024u * 1024u + 257u, 0xA44A'5EED'B10B'0001ull);
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
        if (argc == 4 && std::string_view{argv[1]} == "--retention-snapshot-receiver") { return runRetainedHistorySnapshotReceiver(argv[3], argv[2]); }
        if (argc == 4 && std::string_view{argv[1]} == "--stripe-crash-writer") { return runStripeCrashWriter(argv[2], argv[3]); }
        if (argc == 4 && std::string_view{argv[1]} == "--raft-log-crash-writer") { return runRaftLogCrashWriter(argv[2], argv[3]); }
        if (argc == 4 && std::string_view{argv[1]} == "--retry-crash-writer") { return runRetryCrashWriter(argv[2], argv[3]); }
        const auto executable = std::filesystem::absolute(argv[0]);
        if (argc == 2 && std::string_view{argv[1]} == "--authority-contract") {
            runClusterAuthorityContractTests();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--query-delivery") {
            testQueryDeliverySnapshots(false); testQueryDeliverySnapshots(true); testPartitionQueryDelivery();
            testNativeClusterQueries(ReplicationMode::STRIPE, false, true, true);
            testNativeClusterQueries(ReplicationMode::MIRROR, true, false, true);
            testNativeClusterQueries(ReplicationMode::PARTITIONED, false, false, true);
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--query-resources") {
            runClusterQueryResourceTests();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--rpc-deadlines") {
            runClusterRpcDeadlineTests();
            return 0;
        }
        if (argc == 5 && std::string_view{argv[1]} == "--cluster-query-one") {
            testNativeClusterQueries(static_cast<ReplicationMode>(std::stoi(argv[2])), std::stoi(argv[3]) != 0, std::stoi(argv[4]) != 0);
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--cluster-query-limits") {
            testNativeQueryLimitsAndLifetime();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--cluster-queries") {
            testRaftEngineLeaderAppliesMutationOnce();
            runNativeClusterQueryTests();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--raft-prevote") {
            std::fprintf(stderr, "[cluster] pre-vote wire contract\n");
            testRaftPreVoteWireContract();
            std::fprintf(stderr, "[cluster] pre-vote stale log\n");
            testRaftPreVoteRejectsStaleLog();
            std::fprintf(stderr, "[cluster] pre-vote heartbeat race\n");
            testRaftPreVoteDiscardsProbeAfterHeartbeat();
            std::fprintf(stderr, "[cluster] pre-vote isolation/recovery PLAIN\n");
            testRaftPreVoteIsolationAndRecovery();
            std::fprintf(stderr, "[cluster] pre-vote isolation/recovery SECURE\n");
            testRaftPreVoteIsolationAndRecovery(true);
            std::fprintf(stderr, "[cluster] pre-vote explicit transfer\n");
            testRaftLeaderTransfer();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--stripe-crash-recovery") {
            testStripePublicationCrashRecovery(executable);
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--partition-placement") {
            testClusterPlacementDistribution();
            testPartitionedCatchupTargets(true);
            testPartitionedCatchupTargets(false);
            testPartitionedSingleCopy();
            testPartitionedEmptySnapshots();
            testPartitionedBoundedStorage(false);
            testPartitionedBoundedStorage(true);
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--placement-options") {
            testReconfigurationDeadlineBudget(); testStripePlacementOptions(false); testStripePlacementOptions(true);
            testPartitionRaftLiveCopies(); return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--stripe-online") { testStripeOnlinePlacement(); testStripeOnlinePlacement(true); return 0; }
        if (argc == 2 && std::string_view{argv[1]} == "--local-acceptance-loss") { testMirrorLocalAcceptanceLoss(); return 0; }
        if (argc == 2 && std::string_view{argv[1]} == "--raid10-online") { testRaid10OnlineSpareReplacement(); return 0; }
        if (argc == 2 && std::string_view{argv[1]} == "--partition-query-parallelism") {
            testPartitionQueryQueueDeadline(); testPartitionQueryParallelism(); return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--partition-raft") {
            std::fprintf(stderr, "[cluster] STRIPE online PLAIN\n");
            testStripeOnlinePlacement();
            std::fprintf(stderr, "[cluster] STRIPE online SECURE\n");
            testStripeOnlinePlacement(true);
            std::fprintf(stderr, "[cluster] MIRROR failover policies\n");
            testMirrorFailoverPolicies();
            std::fprintf(stderr, "[cluster] MIRROR local acceptance loss\n");
            testMirrorLocalAcceptanceLoss();
            std::fprintf(stderr, "[cluster] MIRROR online history\n");
            testMirrorOnlineRetainsHistory();
            std::fprintf(stderr, "[cluster] RAID.10 online spare replacement\n");
            testRaid10OnlineSpareReplacement();
            std::fprintf(stderr, "[cluster] PARTITIONED failover\n");
            testPartitionRaftFailover();
            std::fprintf(stderr, "[cluster] PARTITIONED online copy count\n");
            testPartitionRaftLiveCopies();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--replication-catchup") {
            // Focused coverage for bounded bootstrap, live cutover, and worker cleanup.
            testReplicationServerStreamsSnapshotCatchupThroughBoundedQueue();
            testReplicationServerRejectsOversizedCatchupFrame();
            testReplicationServerOrdersWritesCommittedDuringCatchup();
            testReplicationServerReapsDisconnectedReplicas();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--raft-log-retention") {
            testRaftEmptyStateCompaction(); testRaftRetainedHistorySnapshot(argv[0]); testMirrorOnlineRetainsHistory();
            testPartitionRaftLiveCopies(); testStripeOnlinePlacement(); testStripeOnlinePlacement(true); return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--raft-history-snapshot") { testRaftRetainedHistorySnapshot(argv[0]); return 0; }
        if (argc == 2 && std::string_view{argv[1]} == "--mirror-history-snapshot") { testMirrorOnlineRetainsHistory(); return 0; }
        if (argc == 2 && std::string_view{argv[1]} == "--partition-history-snapshot") { testPartitionRaftLiveCopies(); return 0; }
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
        if (argc == 2 && std::string_view{argv[1]} == "--raft-learners") {
            testRaftLearnerValidationAndStaticMembership();
            testRaftLearnerEngineApi();
            testRaftLearnerLifecycle();
            testRaftLearnerLifecycle(true);
            testRaftLearnersNeverSupplyQuorum();
            testRaftLearnerPromotionCrashRecovery(executable);
            return 0;
        }
        if (argc == 4 && std::string_view{argv[1]} == "--learner-promotion-crash-writer") {
            return runLearnerPromotionCrashWriter(argv[2], argv[3]);
        }
        if (argc == 2 && std::string_view{argv[1]} == "--stripe-parallelism") {
            testTargetedAckMatchesRequest();
            testRollbackControlSerialization();
            testStripeMetadataParallelism(false);
            testStripeMetadataParallelism(true);
            testStripeEngineIndependentKeys(false);
            testStripeEngineIndependentKeys(true);
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--raft-log-delta") {
            testRaftLogDeltaIndex();
            testRaftSegmentedLogRecovery(executable);
            testRaftLogTrailingBytesRecoveryPolicy();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--stripe-concurrency") {
            testStripeFailureAndConcurrentReads();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--stripe-read-repair") {
            testStripeReadRepairStatistics();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--stripe-rollback") {
            testStripeDistributedRollback();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--global-rollback") {
            testGlobalStreamDistributedRollback();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--stripe-degraded-rebuild") {
            std::fprintf(stderr, "[stripe degraded] degraded write and automatic rebuild\n");
            testStripeEngineDegradedWriteAndAutomaticRebuild();
            std::fprintf(stderr, "[stripe degraded] metadata leader automatic rebuild\n");
            testStripeMetadataLeaderAutomaticRebuild();
            std::fprintf(stderr, "[stripe degraded] metadata Raft leader failover\n");
            testStripeMetadataRaftLeaderFailover();
            std::fprintf(stderr, "[stripe degraded] RAID.6 two failures\n");
            testRaid6PresetMetadataQuorumSurvivesTwoFailures();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--raid0") {
            testStripeErasureCodecRecovery();
            testPartitionedAndStripeRouting();
            testRaid0PresetStorageAndFailure();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--raid1") {
            testRaid1PresetMirroringAndRebuild();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--raid5") {
            testRaid5PresetDegradedWriteAndRead();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--raid6") {
            testRaid6PresetMetadataQuorumSurvivesTwoFailures();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--raid10") {
            testRaid10PresetMirroredStripeAndRebuild();
            testRaid10HotSpareReplacementAndNoFailback();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--routing") {
            std::fprintf(stderr, "[cluster] routing framing/validation\n");
            testForwardFraming();
            std::fprintf(stderr, "[cluster] routing PARTITIONED PLAIN\n");
            testNativePartitionRouting(false);
            std::fprintf(stderr, "[cluster] routing PARTITIONED SECURE\n");
            testNativePartitionRouting(true);
            std::fprintf(stderr, "[cluster] routing Raft PLAIN/learner\n");
            testNativeRaftRouting(false);
            std::fprintf(stderr, "[cluster] routing Raft SECURE/learner\n");
            testNativeRaftRouting(true);
            std::fprintf(stderr, "[cluster] routing MIRROR strict ACK\n");
            testNativeMirrorAckForwarding();
            std::fprintf(stderr, "[cluster] routing timeout/no replay\n");
            testForwardTimeoutNeverReplays();
            std::fprintf(stderr, "[cluster] routing Raft admission budget\n");
            testRaftForwardAdmissionBudget();
            std::fprintf(stderr, "[cluster] routing STRIPE write/read coordinator\n");
            testStripeEngineOwnerOnlyWritesAndCoordinatorReads();
            std::fprintf(stderr, "[cluster] routing MIRROR quorum authority/ASYNC data\n");
            testMirrorFencingEngineApi();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--partitioned-owner-read") {
            testPartitionedEngineOwnerLinearizableRead();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--partitioned-rollback") {
            testPartitionedDistributedRollback();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--lease-renewal") {
            testClusterManagerContainsLeaseRenewalFailure();
            testClusterRuntimeReportsLeaseRenewalFailure();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--mirror-promotion") {
            testReplicaRejectsImplicitNonRaftGroupSwitch();
            testNonRaftMirrorRejectsSecondPrimary();
            testNonRaftPrimaryReusesGroupStateAfterRestart();
            testNonRaftMirrorOfflinePromotion();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--mirror-completion-recovery") {
            testMirrorQuorumCompletionRecovery();
            testMirrorQuorumCompletionRecovery(false);
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--placement-options-secure") { testStripePlacementOptions(true); return 0; }
        if (argc == 2 && std::string_view{argv[1]} == "--mirror-recovery") {
            testMirrorRecoveryValidation();
            testMirrorManualRecovery(ConsistencyMode::ASYNC);
            testMirrorManualRecovery(ConsistencyMode::PRIMARY_ACK);
            testMirrorAutomaticRecovery();
            testMirrorRecoveryEngineApi();
            testMirrorQuorumPromotion(true);
            testMirrorQuorumCompletionRecovery();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--mirror-fencing") {
            testMirrorFencingValidation();
            testNonRaftMirrorOfflinePromotion();
            testMirrorQuorumFencing();
            testMirrorQuorumFencing(true);
            testMirrorQuorumPromotion(false);
            testMirrorQuorumPromotion(true);
            testMirrorQuorumCompletionRecovery();
            testMirrorQuorumCompletionRecovery(false);
            testMirrorFencingEngineApi();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--runtime-lifecycle") {
            testStripeRuntimeStartsActivePlacement();
            testClusterRuntimeDoesNotSerializePeerReadWithStats();
            testClusterRuntimeRoleChangeDoesNotJoinEndpointUnderStateLock();
            testClusterRuntimeStartCanRetryAfterFailure();
            testClusterRuntimeReportsEndpointStartFailure();
            testClusterRuntimeReportsPeerReadTimeout();
            testClusterRuntimeReportsLeaseRenewalFailure();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--stripe-failover") {
            testStripeOwnerFailoverAndHandoff();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--raft-engine-ledger") {
            testRetrySafeEngineWrites();
            testRaftLogBlobPolicyReplicatesPayload();
            testRaftLogBlobPolicyExternalizesEngineValue();
            testRaftEngineLeaderAppliesMutationOnce();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--file-snapshot-transfer") {
            testFileBackedSnapshotTransfer();
            return 0;
        }
        if (argc == 2 && std::string_view{argv[1]} == "--memory-only-snapshot") {
            testRaftOptionsRoundtripAndValidation();
            testMemoryOnlySnapshotModes();
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
            std::fprintf(stderr, "[cluster] file-backed snapshot transfer\n");
            testFileBackedSnapshotTransfer();
            std::fprintf(stderr, "[cluster] memory-only snapshot modes\n");
            testMemoryOnlySnapshotModes();
            std::fprintf(stderr, "[cluster] large transfers PLAIN/SECURE\n");
            testLargeReplicationTransfer(false);
            testLargeReplicationTransfer(true);
            std::fprintf(stderr, "[cluster] read timeout preserves write ACK connection\n");
            testTimedOutReadKeepsWriteConnection();
            if (argc == 2) { return 0; }
        }
        if (argc == 1 || (argc == 2 && std::string_view{argv[1]} == "--cluster-hardening")) {
            runClusterAuthorityContractTests();
            runClusterQueryResourceTests();
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
        std::fprintf(stderr, "[cluster] testRaid0PresetStorageAndFailure\n");
        testRaid0PresetStorageAndFailure();
        std::fprintf(stderr, "[cluster] testRaid1PresetMirroringAndRebuild\n");
        testRaid1PresetMirroringAndRebuild();
        std::fprintf(stderr, "[cluster] testRaid5PresetDegradedWriteAndRead\n");
        testRaid5PresetDegradedWriteAndRead();
        std::fprintf(stderr, "[cluster] testRaid10PresetMirroredStripeAndRebuild\n");
        testRaid10PresetMirroredStripeAndRebuild();
        std::fprintf(stderr, "[cluster] testRaid10HotSpareReplacementAndNoFailback\n");
        testRaid10HotSpareReplacementAndNoFailback();
        std::fprintf(stderr, "[cluster] testStripeOwnerFailoverAndHandoff\n");
        testStripeOwnerFailoverAndHandoff();
        std::fprintf(stderr, "[cluster] testStripeMetadataLeaderAutomaticRebuild\n");
        testStripeMetadataLeaderAutomaticRebuild();
        std::fprintf(stderr, "[cluster] testStripeMetadataRaftLeaderFailover\n");
        testStripeMetadataRaftLeaderFailover();
        std::fprintf(stderr, "[cluster] testRaid6PresetMetadataQuorumSurvivesTwoFailures\n");
        testRaid6PresetMetadataQuorumSurvivesTwoFailures();
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
        testForwardFraming();
        testNativePartitionRouting(false);
        testNativePartitionRouting(true);
        testNativeRaftRouting(false);
        testNativeRaftRouting(true);
        testNativeMirrorAckForwarding();
        testForwardTimeoutNeverReplays();
        testRaftForwardAdmissionBudget();
        testPartitionedEngineOwnerLinearizableRead();
        std::fprintf(stderr, "[cluster] testPartitionedDistributedRollback\n");
        testPartitionedDistributedRollback();
        std::fprintf(stderr, "[cluster] testStripeEngineOwnerOnlyWritesAndCoordinatorReads\n");
        testStripeEngineOwnerOnlyWritesAndCoordinatorReads();
        std::fprintf(stderr, "[cluster] testStripeDistributedRollback\n");
        testStripeDistributedRollback();
        std::fprintf(stderr, "[cluster] testGlobalStreamDistributedRollback\n");
        testGlobalStreamDistributedRollback();
        std::fprintf(stderr, "[cluster] testStripeEngineDegradedWriteAndAutomaticRebuild\n");
        testStripeEngineDegradedWriteAndAutomaticRebuild();
        std::fprintf(stderr, "[cluster] testStripeEngineRejectsForeignConfiguration\n");
        testStripeEngineRejectsForeignConfiguration();
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
        std::fprintf(stderr, "[cluster] Raft pre-vote\n");
        testRaftPreVoteWireContract();
        testRaftPreVoteRejectsStaleLog();
        testRaftPreVoteDiscardsProbeAfterHeartbeat();
        testRaftPreVoteIsolationAndRecovery();
        testRaftPreVoteIsolationAndRecovery(true);
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
        std::fprintf(stderr, "[cluster] Raft learner lifecycle and quorum\n");
        testRaftLearnerValidationAndStaticMembership();
        testRaftLearnerEngineApi();
        testRaftLearnerLifecycle();
        testRaftLearnerLifecycle(true);
        testRaftLearnersNeverSupplyQuorum();
        testRaftLearnerPromotionCrashRecovery(executable);
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
        std::fprintf(stderr, "[cluster] testClusterRuntimeRoleChangeDoesNotJoinEndpointUnderStateLock\n");
        testClusterRuntimeRoleChangeDoesNotJoinEndpointUnderStateLock();
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
        std::fprintf(stderr, "[cluster] testNonRaftMirrorOfflinePromotion\n");
        testNonRaftMirrorOfflinePromotion();
        testMirrorFencingValidation();
        testMirrorQuorumFencing();
        testMirrorQuorumFencing(true);
        testMirrorQuorumPromotion(false);
        testMirrorQuorumPromotion(true);
        testMirrorQuorumCompletionRecovery();
        testMirrorQuorumCompletionRecovery(false);
        testMirrorFencingEngineApi();
        testMirrorRecoveryValidation();
        testMirrorManualRecovery(ConsistencyMode::ASYNC);
        testMirrorManualRecovery(ConsistencyMode::PRIMARY_ACK);
        testMirrorAutomaticRecovery();
        testMirrorRecoveryEngineApi();
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
        runNativeClusterQueryTests();
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

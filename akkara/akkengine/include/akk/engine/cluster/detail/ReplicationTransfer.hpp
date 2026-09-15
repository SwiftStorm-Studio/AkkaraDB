/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#pragma once

#include "akk/engine/cluster/ReplFraming.hpp"
#include <functional>
#include <array>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_set>

namespace akkaradb::engine::cluster::detail {
    inline constexpr uint32_t TRANSFER_FRAME_LIMIT = 64u * 1024u;

    class AKKARADB_CLUSTER_RUNTIME_API TransferBudget : public std::enable_shared_from_this<TransferBudget> {
    public:
        enum class Resource { MEMORY, SPOOL, ACTIVE };
        explicit TransferBudget(ReplicationTransferOptions options);
        std::shared_ptr<void> reserve(Resource resource, uint64_t amount);
        uint64_t used(Resource resource) const;
        struct Stats {
            uint64_t resumeAttempts = 0;
            uint64_t resumedTransfers = 0;
            uint64_t resumedBytes = 0;
            uint64_t discardedPartials = 0;
            uint64_t retainedPartials = 0;
        };
        [[nodiscard]] Stats stats() const;
        const ReplicationTransferOptions options;
    private:
        struct Lease;
        mutable std::mutex mutex_;
        uint64_t memory_ = 0, spool_ = 0, active_ = 0, persistentRemaining_ = 0;
        uint64_t resumeAttempts_ = 0, resumedTransfers_ = 0, resumedBytes_ = 0, discardedPartials_ = 0;
        std::unordered_set<std::string> claimedPaths_;
        uint64_t& counter(Resource resource);
        friend class PersistentSpool;
    };

    using TransferId = std::array<uint8_t, 32>;

    class AKKARADB_CLUSTER_RUNTIME_API TransferSession {
    public:
        void reset();
        void cancel();
        void setScope(uint64_t groupId, uint64_t localNodeId, uint64_t remoteNodeId);
        [[nodiscard]] std::string scope() const;
        void prepare(const TransferId& id);
        [[nodiscard]] bool notifyReady(const TransferId& id, uint64_t offset);
        [[nodiscard]] std::optional<uint64_t> waitReady(const TransferId& id, uint32_t timeoutMs);
    private:
        mutable std::mutex mutex_;
        std::condition_variable cv_;
        bool cancelled_ = false;
        std::string scope_ = "default";
        std::optional<TransferId> expected_;
        std::optional<std::pair<TransferId, uint64_t>> ready_;
    };

    struct TransferMessage {
        ReplMsgType type{};
        uint8_t flags = 0;
        uint32_t crc = 0;
        std::span<const uint8_t> payload;
        std::shared_ptr<void> storage;
        size_t size() const { return ReplFrameHeader::SIZE + payload.size(); }
    };
    using MessagePtr = std::shared_ptr<const TransferMessage>;
    using SendFrame = std::function<bool(std::span<const uint8_t>)>;
    using ReceiveFrame = std::function<bool(DecodedFrame&)>;

    AKKARADB_CLUSTER_RUNTIME_API void validateTransferOptions(const ReplicationTransferOptions& options);
    AKKARADB_CLUSTER_RUNTIME_API MessagePtr makeMessage(ReplMsgType type, std::span<const std::span<const uint8_t>> parts,
        const std::shared_ptr<TransferBudget>& budget);
    AKKARADB_CLUSTER_RUNTIME_API MessagePtr messageFromWire(std::vector<uint8_t> wire, const std::shared_ptr<TransferBudget>& budget);
    AKKARADB_CLUSTER_RUNTIME_API bool sendMessage(const TransferMessage& message, const std::shared_ptr<TransferBudget>& budget, const SendFrame& send);
    AKKARADB_CLUSTER_RUNTIME_API MessagePtr receiveMessage(const std::shared_ptr<TransferBudget>& budget, const ReceiveFrame& receive);
    AKKARADB_CLUSTER_RUNTIME_API bool sendMessage(const TransferMessage& message, const std::shared_ptr<TransferBudget>& budget,
        const std::shared_ptr<TransferSession>& session, const SendFrame& send);
    AKKARADB_CLUSTER_RUNTIME_API MessagePtr receiveMessage(const std::shared_ptr<TransferBudget>& budget,
        const std::shared_ptr<TransferSession>& session, const ReceiveFrame& receive, const SendFrame& sendControl);
    AKKARADB_CLUSTER_RUNTIME_API MessagePtr entryMessage(uint64_t seq, uint64_t source, ReplOpType op, uint8_t flags,
        std::span<const uint8_t> key, std::span<const uint8_t> value, const std::shared_ptr<TransferBudget>& budget);
    AKKARADB_CLUSTER_RUNTIME_API MessagePtr snapshotMessage(const ReplSnapshotEntry& entry, const std::shared_ptr<TransferBudget>& budget);
    AKKARADB_CLUSTER_RUNTIME_API MessagePtr snapshotMessage(std::span<const uint8_t> key, std::span<const uint8_t> value,
        const std::shared_ptr<TransferBudget>& budget);
    AKKARADB_CLUSTER_RUNTIME_API MessagePtr readResponseMessage(const ReadResponse& response, const std::shared_ptr<TransferBudget>& budget);
    AKKARADB_CLUSTER_RUNTIME_API MessagePtr readRequestMessage(uint64_t id, uint64_t snapshotSeq, std::span<const uint8_t> key, const std::shared_ptr<TransferBudget>& budget);
    AKKARADB_CLUSTER_RUNTIME_API MessagePtr blobMessage(uint64_t seq, uint64_t id, std::span<const uint8_t> value, const std::shared_ptr<TransferBudget>& budget);

    struct EntryView {
        uint64_t seq = 0, sourceNodeId = 0;
        ReplOpType op{};
        uint8_t recordFlags = 0;
        std::span<const uint8_t> key, value;
    };
    AKKARADB_CLUSTER_RUNTIME_API bool entryView(std::span<const uint8_t> payload, EntryView& entry);
    AKKARADB_CLUSTER_RUNTIME_API bool snapshotView(std::span<const uint8_t> payload, std::span<const uint8_t>& key, std::span<const uint8_t>& value);
}

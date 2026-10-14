/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
#pragma once
#include <akk/engine/cluster/ClusterTransaction.hpp>
#include <algorithm>
#include <array>
#include <cstdint>
#include <optional>
#include <span>
#include <stdexcept>
#include <vector>
#include <string_view>
#include <utility>

namespace akkaradb::engine::cluster::detail {
    // One envelope is used by authenticated forwarding and replicated commands.
    // PREPARE data is chunked independently of the forwarding/raft frame limit.
    enum class TransactionAction : uint8_t {
        STATUS, CREATE, ENLIST, START_PREPARE, DECIDE_COMMIT, DECIDE_ABORT, READY, FINISH,
        FREEZE, GET, SCAN, PREPARE, APPEND, SEAL, APPLY, RELEASE, PARTICIPANT_STATUS,
        LOCK_KEY, LOCK_RANGE
    };
    struct TransactionMessage {
        TransactionAction action = TransactionAction::STATUS;
        ClusterTransactionId id{};
        uint16_t participant = 0;
        uint64_t expires = 0, argument = 0;
        std::vector<uint8_t> payload{};
    };
    struct TransactionRecord {
        ClusterTransactionStatus status = ClusterTransactionStatus::OPEN;
        uint64_t expires = 0;
        bool finished = false;
        bool ready = false;
        std::vector<uint16_t> participants;
    };
    struct TransactionWrite {
        std::vector<uint8_t> key;
        std::optional<std::vector<uint8_t>> value;
    };
    struct TransactionPrepare {
        std::vector<TransactionWrite> writes;
    };
    inline void transactionInteger(std::vector<uint8_t>& out, uint64_t value, unsigned bytes = 8) {
        for (unsigned i = 0; i < bytes; ++i) { out.push_back(static_cast<uint8_t>(value >> (i * 8))); }
    }
    inline void transactionBytes(std::vector<uint8_t>& out, std::span<const uint8_t> value) {
        if (value.size() > UINT32_MAX) { throw std::length_error("Cluster transaction: field too large"); }
        transactionInteger(out, value.size(), 4); out.insert(out.end(), value.begin(), value.end());
    }
    class TransactionReader {
        std::span<const uint8_t> bytes_;
        size_t pos_ = 0;
    public:
        explicit TransactionReader(std::span<const uint8_t> bytes) : bytes_{bytes} {}
        uint64_t integer(unsigned count = 8) {
            if (count > 8 || count > bytes_.size() - pos_) { throw std::runtime_error("Cluster transaction: truncated integer"); }
            uint64_t value = 0;
            for (unsigned i = 0; i < count; ++i) { value |= uint64_t{bytes_[pos_++]} << (i * 8); }
            return value;
        }
        std::span<const uint8_t> take(size_t count) {
            if (count > bytes_.size() - pos_) { throw std::runtime_error("Cluster transaction: truncated field"); }
            const auto result = bytes_.subspan(pos_, count); pos_ += count; return result;
        }
        std::vector<uint8_t> field() { const auto value = take(static_cast<size_t>(integer(4))); return {value.begin(), value.end()}; }
        uint32_t count(size_t minimumBytes) {
            const auto value = integer(4);
            if (value > 1'048'576 || (minimumBytes && value > (bytes_.size() - pos_) / minimumBytes)) { throw std::runtime_error("Cluster transaction: invalid count"); }
            return static_cast<uint32_t>(value);
        }
        void finish() const { if (pos_ != bytes_.size()) { throw std::runtime_error("Cluster transaction: trailing data"); } }
    };
    inline std::vector<uint8_t> encodeTransactionMessage(const TransactionMessage& message) {
        std::vector<uint8_t> out{'A', 'K', 'D', 'T', '3', std::to_underlying(message.action)};
        out.insert(out.end(), message.id.begin(), message.id.end());
        transactionInteger(out, message.participant, 2); transactionInteger(out, message.expires);
        transactionInteger(out, message.argument); transactionBytes(out, message.payload); return out;
    }
    inline TransactionMessage decodeTransactionMessage(std::span<const uint8_t> bytes) {
        if (bytes.size() < 44 || !std::ranges::starts_with(bytes, std::string_view{"AKDT3"})) { throw std::runtime_error("Cluster transaction: invalid envelope"); }
        TransactionReader reader{bytes.subspan(5)};
        TransactionMessage result;
        const auto action = reader.integer(1);
        if (action > std::to_underlying(TransactionAction::LOCK_RANGE)) { throw std::runtime_error("Cluster transaction: invalid action"); }
        result.action = static_cast<TransactionAction>(action);
        const auto id = reader.take(result.id.size()); std::ranges::copy(id, result.id.begin());
        result.participant = static_cast<uint16_t>(reader.integer(2)); result.expires = reader.integer();
        result.argument = reader.integer(); result.payload = reader.field(); reader.finish(); return result;
    }
    inline std::vector<uint8_t> encodeTransactionPrepare(const TransactionPrepare& prepare) {
        std::vector<uint8_t> out{'A', 'K', 'D', 'P', '1'};
        transactionInteger(out, prepare.writes.size(), 4);
        for (const auto& write : prepare.writes) {
            transactionBytes(out, write.key); out.push_back(static_cast<uint8_t>(write.value.has_value()));
            transactionBytes(out, write.value ? std::span<const uint8_t>{*write.value} : std::span<const uint8_t>{});
        }
        return out;
    }
    inline TransactionPrepare decodeTransactionPrepare(std::span<const uint8_t> bytes) {
        if (bytes.size() < 9 || !std::ranges::starts_with(bytes, std::string_view{"AKDP1"})) { throw std::runtime_error("Cluster transaction: invalid prepared values"); }
        TransactionReader reader{bytes.subspan(5)}; TransactionPrepare result;
        const auto writes = reader.count(9);
        for (uint32_t i = 0; i < writes; ++i) {
            TransactionWrite write; write.key = reader.field(); const auto present = reader.integer(1);
            auto value = reader.field();
            if (present > 1 || write.key.size() > UINT16_MAX || (!present && !value.empty())) { throw std::runtime_error("Cluster transaction: malformed write"); }
            if (!result.writes.empty() && result.writes.back().key >= write.key) { throw std::runtime_error("Cluster transaction: unordered writes"); }
            if (present) { write.value = std::move(value); }
            result.writes.push_back(std::move(write));
        }
        reader.finish(); return result;
    }
}

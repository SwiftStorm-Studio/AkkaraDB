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

// akkengine/src/engine/cluster/ClusterConfig.cpp
#include "akk/engine/cluster/ClusterConfig.hpp"

#include <chrono>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <unordered_set>

#include "akk/cpu/CRC32C.hpp"

namespace akkaradb::engine::cluster {
    namespace {
        constexpr size_t HEADER_SIZE = 32;

        uint64_t now_us() noexcept {
            return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()
            ).count());
        }

        void write_u16(uint8_t* b, size_t off, uint16_t v) noexcept {
            b[off] = static_cast<uint8_t>(v);
            b[off + 1] = static_cast<uint8_t>(v >> 8);
        }

        void write_u32(uint8_t* b, size_t off, uint32_t v) noexcept {
            b[off] = static_cast<uint8_t>(v);
            b[off + 1] = static_cast<uint8_t>(v >> 8);
            b[off + 2] = static_cast<uint8_t>(v >> 16);
            b[off + 3] = static_cast<uint8_t>(v >> 24);
        }

        void write_u64(uint8_t* b, size_t off, uint64_t v) noexcept {
            for (size_t i = 0; i < 8; ++i) { b[off + i] = static_cast<uint8_t>(v >> (8 * i)); }
        }

        uint16_t read_u16(const uint8_t* b, size_t off) noexcept {
            return static_cast<uint16_t>(b[off]) | static_cast<uint16_t>(static_cast<uint16_t>(b[off + 1]) << 8);
        }

        uint32_t read_u32(const uint8_t* b, size_t off) noexcept {
            return static_cast<uint32_t>(b[off]) | (static_cast<uint32_t>(b[off + 1]) << 8) | (static_cast<uint32_t>(b[off + 2]) << 16) | (
                static_cast<uint32_t>(b[off + 3]) << 24);
        }

        uint64_t read_u64(const uint8_t* b, size_t off) noexcept {
            uint64_t v = 0;
            for (size_t i = 0; i < 8; ++i) { v |= static_cast<uint64_t>(b[off + i]) << (8 * i); }
            return v;
        }

        uint32_t crc_file_image(std::vector<uint8_t> bytes) {
            if (bytes.size() >= 28) { write_u32(bytes.data(), 24, 0); }
            return cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size());
        }
    } // namespace

    ClusterConfig::ClusterConfig(std::vector<NodeInfo> nodes, ReplicationMode mode, AckPolicy ack_policy)
        : nodes_{std::move(nodes)}, mode_{mode}, ack_policy_{ack_policy} { validate(); }

    ClusterConfig ClusterConfig::load(const std::filesystem::path& path) {
        std::ifstream file(path, std::ios::binary);
        if (!file) { throw std::runtime_error("ClusterConfig: cannot open " + path.string()); }

        std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
        if (bytes.size() < HEADER_SIZE) { throw std::runtime_error("ClusterConfig: file too short"); }

        const uint32_t magic = read_u32(bytes.data(), 0);
        const uint16_t version = read_u16(bytes.data(), 4);
        if (magic != MAGIC) { throw std::runtime_error("ClusterConfig: bad magic"); }
        if (version != VERSION) { throw std::runtime_error("ClusterConfig: unsupported version"); }

        const uint32_t stored_crc = read_u32(bytes.data(), 24);
        if (stored_crc != crc_file_image(bytes)) { throw std::runtime_error("ClusterConfig: CRC mismatch"); }

        const uint16_t flags = read_u16(bytes.data(), 6);
        const uint16_t node_count = read_u16(bytes.data(), 8);
        const auto mode = static_cast<ReplicationMode>(bytes[10]);
        AckPolicy ack{};
        ack.mode = static_cast<AckPolicyMode>(bytes[11]);
        ack.quorum = read_u16(bytes.data(), 12);

        size_t cursor = HEADER_SIZE;
        std::vector<NodeInfo> nodes;
        nodes.reserve(node_count);
        for (uint16_t i = 0; i < node_count; ++i) {
            if (cursor + 18 > bytes.size()) { throw std::runtime_error("ClusterConfig: truncated node entry"); }
            NodeInfo node{};
            node.node_id = read_u64(bytes.data(), cursor);
            node.capabilities = read_u32(bytes.data(), cursor + 8);
            node.data_port = read_u16(bytes.data(), cursor + 12);
            node.repl_port = read_u16(bytes.data(), cursor + 14);
            const uint16_t host_len = read_u16(bytes.data(), cursor + 16);
            cursor += 18;
            if (cursor + host_len > bytes.size()) { throw std::runtime_error("ClusterConfig: truncated host"); }
            node.host.assign(reinterpret_cast<const char*>(bytes.data() + cursor), host_len);
            cursor += host_len;
            nodes.push_back(std::move(node));
        }

        ClusterConfig cfg{std::move(nodes), mode, ack};
        cfg.flags_ = flags;
        cfg.validate();
        return cfg;
    }

    void ClusterConfig::save(const std::filesystem::path& path, const ClusterConfig& config) {
        config.validate();
        if (path.has_parent_path()) { std::filesystem::create_directories(path.parent_path()); }

        std::vector<uint8_t> bytes(HEADER_SIZE);
        write_u32(bytes.data(), 0, MAGIC);
        write_u16(bytes.data(), 4, VERSION);
        write_u16(bytes.data(), 6, config.flags_);
        write_u16(bytes.data(), 8, static_cast<uint16_t>(config.nodes_.size()));
        bytes[10] = static_cast<uint8_t>(config.mode_);
        bytes[11] = static_cast<uint8_t>(config.ack_policy_.mode);
        write_u16(bytes.data(), 12, config.ack_policy_.quorum);
        write_u64(bytes.data(), 16, now_us());
        write_u32(bytes.data(), 24, 0);

        for (const auto& node : config.nodes_) {
            const auto host_len = static_cast<uint16_t>(node.host.size());
            const size_t off = bytes.size();
            bytes.resize(off + 18 + host_len);
            write_u64(bytes.data(), off, node.node_id);
            write_u32(bytes.data(), off + 8, node.capabilities);
            write_u16(bytes.data(), off + 12, node.data_port);
            write_u16(bytes.data(), off + 14, node.repl_port);
            write_u16(bytes.data(), off + 16, host_len);
            std::memcpy(bytes.data() + off + 18, node.host.data(), host_len);
        }

        write_u32(bytes.data(), 24, crc_file_image(bytes));

        const auto tmp_path = path.parent_path() / (path.filename().string() + ".tmp");
        {
            std::ofstream out(tmp_path, std::ios::binary | std::ios::trunc);
            if (!out) { throw std::runtime_error("ClusterConfig: cannot create " + tmp_path.string()); }
            out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
            if (!out) { throw std::runtime_error("ClusterConfig: write failed"); }
        }
        std::filesystem::rename(tmp_path, path);
    }

    const NodeInfo* ClusterConfig::find_by_id(uint64_t node_id) const noexcept {
        for (const auto& node : nodes_) { if (node.node_id == node_id) { return &node; } }
        return nullptr;
    }

    std::vector<NodeInfo> ClusterConfig::data_nodes() const {
        std::vector<NodeInfo> result;
        for (const auto& node : nodes_) { if (node.data_bearing()) { result.push_back(node); } }
        return result;
    }

    std::vector<NodeInfo> ClusterConfig::coordinator_nodes() const {
        std::vector<NodeInfo> result;
        for (const auto& node : nodes_) { if (node.coordinator_eligible()) { result.push_back(node); } }
        return result;
    }

    bool ClusterConfig::is_standalone() const noexcept { return mode_ == ReplicationMode::Standalone || nodes_.size() <= 1; }

    void ClusterConfig::validate() const {
        if (nodes_.size() > UINT16_MAX) { throw std::invalid_argument("ClusterConfig: too many nodes"); }
        if (mode_ != ReplicationMode::Standalone && mode_ != ReplicationMode::Mirror && mode_ != ReplicationMode::Stripe) {
            throw std::invalid_argument("ClusterConfig: invalid replication mode");
        }
        if (ack_policy_.mode != AckPolicyMode::Async && ack_policy_.mode != AckPolicyMode::All && ack_policy_.mode !=
            AckPolicyMode::Quorum) { throw std::invalid_argument("ClusterConfig: invalid ack policy"); }
        if (ack_policy_.mode == AckPolicyMode::Quorum && ack_policy_.quorum == 0) {
            throw std::invalid_argument("ClusterConfig: quorum policy requires quorum > 0");
        }

        std::unordered_set<uint64_t> ids;
        bool has_data_node = false;
        bool has_coordinator = false;
        for (const auto& node : nodes_) {
            if (node.node_id == 0) { throw std::invalid_argument("ClusterConfig: node_id 0 is reserved"); }
            if (!ids.insert(node.node_id).second) { throw std::invalid_argument("ClusterConfig: duplicate node_id"); }
            if (node.host.empty()) { throw std::invalid_argument("ClusterConfig: empty host"); }
            if ((node.capabilities & ~(CoordinatorEligible | DataBearing)) != 0) {
                throw std::invalid_argument("ClusterConfig: unknown node capability");
            }
            has_data_node = has_data_node || node.data_bearing();
            has_coordinator = has_coordinator || node.coordinator_eligible();
        }
        if (mode_ != ReplicationMode::Standalone && !has_data_node) {
            throw std::invalid_argument("ClusterConfig: cluster mode requires a data-bearing node");
        }
        if (mode_ != ReplicationMode::Standalone && !has_coordinator) {
            throw std::invalid_argument("ClusterConfig: cluster mode requires a coordinator-eligible node");
        }
    }
} // namespace akkaradb::engine::cluster

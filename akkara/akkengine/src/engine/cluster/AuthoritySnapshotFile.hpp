/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
#pragma once
#include "akk/engine/cluster/ClusterRuntimeProvider.hpp"
#include "akk/cpu/CRC32C.hpp"
#include <fstream>

namespace akkaradb::engine::cluster::detail {
    // Authority entries are bounded metadata, never shard/Blob payloads.
    // The complete spool is synced before Raft records its install intent.
    class AuthoritySnapshotFile {
        static constexpr uint64_t MAX_ENTRY = 4ull * 1024 * 1024;
        std::filesystem::path path_;
        std::ofstream out_;
        std::vector<uint8_t> key_, value_;
        uint64_t seq_ = 0, count_ = 0, size_ = 0;
        uint32_t valueCrc_ = 0;
        bool active_ = false;
        static uint32_t crc(std::span<const uint8_t> bytes) {
            return cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size());
        }
        static void integer(std::ostream& out, uint64_t value, size_t width) {
            for (size_t i = 0; i < width; ++i) { out.put(static_cast<char>(value >> (8 * i))); }
        }
        static uint64_t integer(std::istream& in, size_t width) {
            uint64_t value = 0;
            for (size_t i = 0; i < width; ++i) {
                const auto byte = in.get();
                if (byte == std::char_traits<char>::eof()) { throw std::runtime_error("Authority snapshot: truncated file"); }
                value |= uint64_t{static_cast<uint8_t>(byte)} << (8 * i);
            }
            return value;
        }
    public:
        explicit AuthoritySnapshotFile(std::filesystem::path path) : path_{std::move(path)} {}
        const auto& path() const { return path_; }
        void begin(uint64_t seq) {
            if (out_.is_open()) { out_.close(); }
            std::filesystem::create_directories(path_.parent_path());
            out_.open(path_, std::ios::binary | std::ios::trunc);
            if (!out_) { throw std::runtime_error("Authority snapshot: cannot create staging file"); }
            integer(out_, seq, 8); integer(out_, 0, 8);
            seq_ = seq; count_ = 0; active_ = false;
        }
        void beginEntry(std::span<const uint8_t> key, uint64_t size, uint32_t valueCrc) {
            if (!out_.is_open() || active_ || key.size() > MAX_ENTRY || size > MAX_ENTRY) {
                throw std::runtime_error("Authority snapshot: invalid entry");
            }
            key_.assign(key.begin(), key.end()); value_.clear(); size_ = size; valueCrc_ = valueCrc; active_ = true;
        }
        void append(uint64_t offset, std::span<const uint8_t> chunk) {
            if (!active_ || offset != value_.size() || chunk.size() > size_ - offset) {
                throw std::runtime_error("Authority snapshot: invalid chunk");
            }
            value_.insert(value_.end(), chunk.begin(), chunk.end());
        }
        void finishEntry() {
            if (!active_ || value_.size() != size_ || crc(value_) != valueCrc_) { throw std::runtime_error("Authority snapshot: invalid checksum"); }
            integer(out_, key_.size(), 8); integer(out_, value_.size(), 8);
            integer(out_, crc(key_), 4); integer(out_, valueCrc_, 4);
            out_.write(reinterpret_cast<const char*>(key_.data()), static_cast<std::streamsize>(key_.size()));
            out_.write(reinterpret_cast<const char*>(value_.data()), static_cast<std::streamsize>(value_.size()));
            if (!out_) { throw std::runtime_error("Authority snapshot: write failed"); }
            ++count_; active_ = false;
        }
        void prepare(uint64_t seq, uint64_t count) {
            if (seq != seq_ || count != count_ || active_) { throw std::runtime_error("Authority snapshot: incomplete receive"); }
            if (!out_.is_open()) { return; }
            out_.seekp(8); integer(out_, count, 8); out_.flush();
            if (!out_) { throw std::runtime_error("Authority snapshot: flush failed"); }
            out_.close();
        }
        bool replay(uint64_t seq, const SnapshotEntryVisitor& visitor) const {
            std::ifstream in{path_, std::ios::binary};
            if (integer(in, 8) != seq) { throw std::runtime_error("Authority snapshot: sequence mismatch"); }
            const auto count = integer(in, 8);
            for (uint64_t i = 0; i < count; ++i) {
                const auto keySize = integer(in, 8), valueSize = integer(in, 8);
                const auto keyCrc = integer(in, 4), valueCrc = integer(in, 4);
                if (keySize > MAX_ENTRY || valueSize > MAX_ENTRY) { throw std::runtime_error("Authority snapshot: oversized entry"); }
                std::vector<uint8_t> key(static_cast<size_t>(keySize)), value(static_cast<size_t>(valueSize));
                in.read(reinterpret_cast<char*>(key.data()), static_cast<std::streamsize>(key.size()));
                in.read(reinterpret_cast<char*>(value.data()), static_cast<std::streamsize>(value.size()));
                if (!in || crc(key) != keyCrc || crc(value) != valueCrc) { throw std::runtime_error("Authority snapshot: corrupt staging entry"); }
                if (!visitor.beginEntry(key, value.size(), static_cast<uint32_t>(valueCrc)) ||
                    !visitor.appendValueChunk(0, value) || !visitor.finishEntry()) { return false; }
            }
            if (in.peek() != std::char_traits<char>::eof()) { throw std::runtime_error("Authority snapshot: trailing data"); }
            return true;
        }
    };
}

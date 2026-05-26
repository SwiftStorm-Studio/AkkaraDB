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

// akkengine/src/engine/vlog/VersionLog.cpp
#include "akk/engine/vlog/VersionLog.hpp"

#include "akk/cpu/CRC32C.hpp"
#include "akk/core/record/KeyFingerprint.hpp"

#include <algorithm>
#include <limits>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

namespace akkaradb::engine::vlog {
    namespace fs = std::filesystem;

    namespace {
        static constexpr uint32_t AKVLOG_V5_MAGIC = 0x35564B41u; // "AKV5"
        static constexpr uint16_t AKVLOG_V5_VERSION = 0x0001u;

        #pragma pack(push, 1)
        struct AkvlogV5FileHeader {
            uint32_t magic;
            uint16_t version;
            uint8_t sync_mode_hint;
            uint8_t reserved0;
            uint64_t created_ns;
            uint64_t reserved1;
            uint32_t crc32c;
            uint32_t reserved2;
        };
        #pragma pack(pop)

        #pragma pack(push, 1)
        struct AkvlogV5EntryHeader {
            uint32_t entry_len;
            uint64_t seq;
            uint64_t source_node_id;
            uint64_t timestamp_ns;
            uint8_t flags;
            uint64_t key_fp64;
            uint16_t key_len;
            uint32_t value_len;
        };
        #pragma pack(pop)

        static_assert(sizeof(AkvlogV5FileHeader) == 32);
        static_assert(sizeof(AkvlogV5EntryHeader) == 43);

        static constexpr size_t ENTRY_HDR_SIZE = sizeof(AkvlogV5EntryHeader);
        static constexpr size_t CRC_SIZE = sizeof(uint32_t);
        static constexpr size_t MIN_ENTRY_SIZE = ENTRY_HDR_SIZE + CRC_SIZE;
        static constexpr size_t MAX_ENTRY_SIZE = 32u * 1024u * 1024u;

        [[nodiscard]] static uint64_t now_ns_fallback() noexcept { return 0; }

        static void do_fdatasync(FILE* f) {
            #ifdef _WIN32
            _commit(_fileno(f));
            #else
            fdatasync(fileno(f));
            #endif
        }
    } // namespace

    class VersionLog::Impl {
        public:
            VersionLogOptions opts_;
            mutable std::mutex mu_;

            struct StringViewHash {
                using is_transparent = void;
                size_t operator()(std::string_view sv) const noexcept { return std::hash<std::string_view>{}(sv); }
                size_t operator()(const std::string& s) const noexcept { return std::hash<std::string_view>{}(s); }
            };

            std::unordered_map<std::string, std::vector<VersionEntry>, StringViewHash, std::equal_to<>> index_;
            FILE* file_ = nullptr;
            std::vector<uint8_t> write_buf_;

            void serialize_and_write(
                FILE* f,
                const uint8_t* key_data,
                size_t key_len,
                uint64_t seq,
                uint64_t source_node_id,
                uint64_t timestamp_ns,
                uint8_t flags,
                const uint8_t* value_data,
                size_t value_len
            ) {
                const size_t total = ENTRY_HDR_SIZE + key_len + value_len + CRC_SIZE;
                if (total > MAX_ENTRY_SIZE) { throw std::invalid_argument("VersionLog: entry too large"); }
                if (key_len > std::numeric_limits<uint16_t>::max()) { throw std::invalid_argument("VersionLog: key too large"); }
                if (value_len > std::numeric_limits<uint32_t>::max()) { throw std::invalid_argument("VersionLog: value too large"); }

                const uint32_t entry_len = static_cast<uint32_t>(total);
                write_buf_.resize(entry_len);
                uint8_t* p = write_buf_.data();

                auto& hdr = *reinterpret_cast<AkvlogV5EntryHeader*>(p);
                hdr.entry_len = entry_len;
                hdr.seq = seq;
                hdr.source_node_id = source_node_id;
                hdr.timestamp_ns = timestamp_ns;
                hdr.flags = flags;
                hdr.key_fp64 = key_len == 0 ? 0ULL : core::compute_key_fp64(key_data, key_len);
                hdr.key_len = static_cast<uint16_t>(key_len);
                hdr.value_len = static_cast<uint32_t>(value_len);
                p += ENTRY_HDR_SIZE;

                if (key_len > 0) {
                    std::memcpy(p, key_data, key_len);
                    p += key_len;
                }
                if (value_len > 0) {
                    std::memcpy(p, value_data, value_len);
                    p += value_len;
                }

                std::memset(p, 0, CRC_SIZE);
                const uint32_t crc = cpu::CRC32C(reinterpret_cast<const std::byte*>(write_buf_.data()), entry_len - CRC_SIZE);
                std::memcpy(p, &crc, CRC_SIZE);

                if (fwrite(write_buf_.data(), 1, entry_len, f) != entry_len) { throw std::runtime_error("VersionLog: fwrite failed"); }
            }

            void write_entry(
                std::span<const uint8_t> key,
                uint64_t seq,
                uint64_t source_node_id,
                uint64_t timestamp_ns,
                uint8_t flags,
                std::span<const uint8_t> value
            ) {
                serialize_and_write(file_, key.data(), key.size(), seq, source_node_id, timestamp_ns, flags, value.data(), value.size());
                fflush(file_);
                if (opts_.sync_mode == VLogSyncMode::Sync) { do_fdatasync(file_); }
            }

            void write_file_header(FILE* wf) {
                AkvlogV5FileHeader hdr{};
                hdr.magic = AKVLOG_V5_MAGIC;
                hdr.version = AKVLOG_V5_VERSION;
                hdr.sync_mode_hint = static_cast<uint8_t>(opts_.sync_mode);
                hdr.reserved0 = 0;
                hdr.created_ns = now_ns_fallback();
                hdr.reserved1 = 0;
                hdr.crc32c = 0;
                hdr.reserved2 = 0;
                hdr.crc32c = cpu::CRC32C(reinterpret_cast<const std::byte*>(&hdr), sizeof(hdr));

                if (fwrite(&hdr, sizeof(hdr), 1, wf) != 1) { throw std::runtime_error("VersionLog: failed to write header"); }
                fflush(wf);
                do_fdatasync(wf);
            }

            void open_or_create() {
                const auto& path = opts_.log_path;
                const auto parent = path.parent_path();
                if (!parent.empty()) { fs::create_directories(parent); }

                const bool existed = fs::exists(path);
                if (existed) {
                    FILE* rf = nullptr;
                    #ifdef _WIN32
                    rf = _wfopen(path.wstring().c_str(), L"rb");
                    #else
                    rf = fopen(path.string().c_str(), "rb");
                    #endif
                    if (rf != nullptr) {
                        recover(rf);
                        fclose(rf);
                    }
                }

                #ifdef _WIN32
                file_ = _wfopen(path.wstring().c_str(), L"ab");
                #else
                file_ = fopen(path.string().c_str(), "ab");
                #endif
                if (!file_) { throw std::runtime_error("VersionLog: cannot open file: " + path.string()); }

                if (!existed || fs::file_size(path) == 0) { write_file_header(file_); }
            }

            void insert_sorted(const std::string& key, VersionEntry ve) {
                auto& versions = index_[key];
                if (versions.empty() || versions.back().seq <= ve.seq) {
                    versions.push_back(std::move(ve));
                    return;
                }

                const auto pos = std::lower_bound(versions.begin(), versions.end(), ve.seq, [](const VersionEntry& e, uint64_t seq) { return e.seq < seq; });
                versions.insert(pos, std::move(ve));
            }

            void recover(FILE* rf) {
                AkvlogV5FileHeader file_hdr{};
                if (fread(&file_hdr, sizeof(file_hdr), 1, rf) != 1) { return; }

                const uint32_t stored_header_crc = file_hdr.crc32c;
                file_hdr.crc32c = 0;
                const uint32_t computed_header_crc = cpu::CRC32C(reinterpret_cast<const std::byte*>(&file_hdr), sizeof(file_hdr));
                if (file_hdr.magic != AKVLOG_V5_MAGIC || file_hdr.version != AKVLOG_V5_VERSION || stored_header_crc != computed_header_crc) { return; }

                std::vector<uint8_t> buf;
                while (true) {
                    uint32_t entry_len = 0;
                    if (fread(&entry_len, sizeof(entry_len), 1, rf) != 1) { break; }
                    if (entry_len < MIN_ENTRY_SIZE || entry_len > MAX_ENTRY_SIZE) { break; }

                    buf.resize(entry_len);
                    std::memcpy(buf.data(), &entry_len, sizeof(entry_len));

                    const size_t rest = entry_len - sizeof(entry_len);
                    if (fread(buf.data() + sizeof(entry_len), 1, rest, rf) != rest) { break; }

                    uint32_t stored_entry_crc = 0;
                    std::memcpy(&stored_entry_crc, buf.data() + entry_len - CRC_SIZE, CRC_SIZE);
                    std::memset(buf.data() + entry_len - CRC_SIZE, 0, CRC_SIZE);
                    const uint32_t computed_entry_crc = cpu::CRC32C(reinterpret_cast<const std::byte*>(buf.data()), entry_len - CRC_SIZE);
                    if (stored_entry_crc != computed_entry_crc) { continue; }

                    if (buf.size() < ENTRY_HDR_SIZE) { continue; }
                    const auto& ehdr = *reinterpret_cast<const AkvlogV5EntryHeader*>(buf.data());
                    const size_t expected_size = ENTRY_HDR_SIZE + ehdr.key_len + ehdr.value_len + CRC_SIZE;
                    if (expected_size != entry_len) { continue; }

                    const uint8_t* p = buf.data() + ENTRY_HDR_SIZE;
                    std::string key(reinterpret_cast<const char*>(p), ehdr.key_len);
                    p += ehdr.key_len;

                    VersionEntry ve;
                    ve.seq = ehdr.seq;
                    ve.source_node_id = ehdr.source_node_id;
                    ve.timestamp_ns = ehdr.timestamp_ns;
                    ve.flags = ehdr.flags;
                    ve.value.assign(p, p + ehdr.value_len);
                    insert_sorted(key, std::move(ve));
                }
            }
    };

    std::unique_ptr<VersionLog> VersionLog::create(VersionLogOptions opts) {
        auto log = std::unique_ptr<VersionLog>(new VersionLog{});
        log->impl_ = std::make_unique<Impl>();
        log->impl_->opts_ = std::move(opts);
        log->impl_->write_buf_.reserve(4096);
        log->impl_->open_or_create();
        return log;
    }

    VersionLog::~VersionLog() { close(); }

    void VersionLog::append(
        std::span<const uint8_t> key,
        uint64_t seq,
        uint64_t source_node_id,
        uint64_t timestamp_ns,
        uint8_t flags,
        std::span<const uint8_t> value
    ) {
        if (!impl_) { return; }

        std::lock_guard lock{impl_->mu_};
        if (!impl_->file_) { return; }

        impl_->write_entry(key, seq, source_node_id, timestamp_ns, flags, value);

        const std::string key_str(reinterpret_cast<const char*>(key.data()), key.size());
        VersionEntry ve;
        ve.seq = seq;
        ve.source_node_id = source_node_id;
        ve.timestamp_ns = timestamp_ns;
        ve.flags = flags;
        ve.value.assign(value.begin(), value.end());
        impl_->insert_sorted(key_str, std::move(ve));
    }

    std::optional<VersionEntry> VersionLog::get_at(std::span<const uint8_t> key, uint64_t at_seq) const {
        if (!impl_) { return std::nullopt; }

        std::lock_guard lock{impl_->mu_};
        const std::string_view key_sv(reinterpret_cast<const char*>(key.data()), key.size());
        const auto it = impl_->index_.find(key_sv);
        if (it == impl_->index_.end()) { return std::nullopt; }

        const auto& versions = it->second;
        const auto pos = std::upper_bound(versions.begin(), versions.end(), at_seq, [](uint64_t seq, const VersionEntry& e) { return seq < e.seq; });
        if (pos == versions.begin()) { return std::nullopt; }
        return *std::prev(pos);
    }

    std::vector<VersionEntry> VersionLog::history(std::span<const uint8_t> key) const {
        if (!impl_) { return {}; }

        std::lock_guard lock{impl_->mu_};
        const std::string_view key_sv(reinterpret_cast<const char*>(key.data()), key.size());
        const auto it = impl_->index_.find(key_sv);
        if (it == impl_->index_.end()) { return {}; }
        return it->second;
    }

    std::vector<std::pair<std::vector<uint8_t>, std::optional<VersionEntry>>> VersionLog::collect_rollback_targets(uint64_t target_seq) const {
        if (!impl_) { return {}; }

        std::lock_guard lock{impl_->mu_};
        std::vector<std::pair<std::vector<uint8_t>, std::optional<VersionEntry>>> result;
        for (const auto& [key, versions] : impl_->index_) {
            if (versions.empty() || versions.back().seq <= target_seq) { continue; }

            const auto pos = std::upper_bound(versions.begin(), versions.end(), target_seq, [](uint64_t seq, const VersionEntry& e) { return seq < e.seq; });

            std::optional<VersionEntry> prev;
            if (pos != versions.begin()) { prev = *std::prev(pos); }

            std::vector<uint8_t> key_bytes(key.begin(), key.end());
            result.emplace_back(std::move(key_bytes), std::move(prev));
        }

        return result;
    }

    void VersionLog::close() {
        if (!impl_) { return; }
        std::lock_guard lock{impl_->mu_};
        if (!impl_->file_) { return; }
        fflush(impl_->file_);
        fclose(impl_->file_);
        impl_->file_ = nullptr;
    }
} // namespace akkaradb::engine::vlog

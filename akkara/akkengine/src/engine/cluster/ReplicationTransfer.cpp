/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

#include "akk/engine/cluster/detail/ReplicationTransfer.hpp"
#include "akk/cpu/CRC32C.hpp"
#include "akk/crypto/Random.hpp"
#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <stdexcept>
#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#else
#include <cerrno>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace akkaradb::engine::cluster::detail {
    namespace {
        constexpr size_t TRANSFER_BEGIN_SIZE = 46;
        constexpr size_t TRANSFER_READY_SIZE = 40;

        uint64_t readLe(std::span<const uint8_t> bytes, size_t offset, size_t count) {
            uint64_t value = 0;
            for (size_t i = 0; i < count; ++i) { value |= uint64_t{bytes[offset + i]} << (i * 8); }
            return value;
        }
        void appendLe(std::vector<uint8_t>& bytes, uint64_t value, size_t count) {
            for (size_t i = 0; i < count; ++i) { bytes.push_back(static_cast<uint8_t>(value >> (i * 8))); }
        }
        uint32_t checksum(std::span<const uint8_t> bytes) {
            return cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size());
        }
        bool transferable(ReplMsgType type) {
            return type == ReplMsgType::ENTRY || type == ReplMsgType::BLOB_PUT || type == ReplMsgType::SNAPSHOT_ENTRY ||
                   type == ReplMsgType::READ_REQUEST || type == ReplMsgType::READ_RESPONSE;
        }
        std::filesystem::path baseSpoolDirectory(const ReplicationTransferOptions& options) {
            return options.spoolDirectory.empty() ? std::filesystem::temp_directory_path() : options.spoolDirectory;
        }
        std::filesystem::path resumeDirectory(const ReplicationTransferOptions& options) {
            return baseSpoolDirectory(options) / "akkaradb-transfer-resume-v1";
        }
        template<size_t N>
        std::string idHex(const std::array<uint8_t, N>& id) {
            static constexpr char HEX[] = "0123456789abcdef";
            std::string out;
            out.reserve(id.size() * 2);
            for (const auto byte : id) { out.push_back(HEX[byte >> 4]); out.push_back(HEX[byte & 15]); }
            return out;
        }
        bool isPartial(const std::filesystem::path& path) { return path.extension() == ".part"; }
        uint64_t fileSize(const std::filesystem::path& path) {
            std::error_code ec;
            const auto size = std::filesystem::file_size(path, ec);
            return ec ? 0 : static_cast<uint64_t>(size);
        }
        uint64_t diskUsage(const TransferBudget& budget) {
            std::error_code ec;
            uint64_t total = 0;
            for (std::filesystem::recursive_directory_iterator it{resumeDirectory(budget.options), ec}; !ec && it != std::filesystem::recursive_directory_iterator{}; it.increment(ec)) {
                if (!it->is_regular_file(ec) || !isPartial(it->path())) { ec.clear(); continue; }
                const auto size = it->file_size(ec);
                if (!ec && size <= UINT64_MAX - total) { total += static_cast<uint64_t>(size); }
                ec.clear();
            }
            return total;
        }
        uint64_t partialCount(const TransferBudget& budget) {
            std::error_code ec;
            uint64_t count = 0;
            for (std::filesystem::recursive_directory_iterator it{resumeDirectory(budget.options), ec}; !ec && it != std::filesystem::recursive_directory_iterator{}; it.increment(ec)) {
                if (it->is_regular_file(ec) && isPartial(it->path())) { ++count; }
                ec.clear();
            }
            return count;
        }
        TransferId makeTransferId(const TransferMessage& message) {
            std::array<uint8_t, 14> metadata{};
            metadata[0] = static_cast<uint8_t>(message.type);
            metadata[1] = message.flags;
            for (size_t i = 0; i < 8; ++i) { metadata[2 + i] = static_cast<uint8_t>(message.payload.size() >> (i * 8)); }
            for (size_t i = 0; i < 4; ++i) { metadata[10 + i] = static_cast<uint8_t>(message.crc >> (i * 8)); }
            const std::array<std::span<const uint8_t>, 2> parts{metadata, message.payload};
            return crypto::hash256(parts);
        }
        bool decodeReady(const DecodedFrame& frame, TransferId& id, uint64_t& offset) {
            if (frame.type != ReplMsgType::TRANSFER_READY || frame.flags != 0 || frame.payload.size() != TRANSFER_READY_SIZE) { return false; }
            std::copy_n(frame.payload.begin(), id.size(), id.begin());
            offset = readLe(frame.payload, id.size(), 8);
            return true;
        }
        bool sendEncoded(ReplMsgType type, std::span<const uint8_t> payload, const std::shared_ptr<TransferBudget>& budget, const SendFrame& send) {
            auto memory = budget->reserve(TransferBudget::Resource::MEMORY, ReplFrameHeader::SIZE + payload.size());
            const auto wire = encodeFrame(type, payload);
            return send(wire);
        }

        class SpoolBase {
        public:
            virtual ~SpoolBase() = default;
            virtual void append(std::span<const uint8_t>) = 0;
            virtual std::span<const uint8_t> finish() = 0;
            virtual uint64_t written() const noexcept = 0;
            virtual void discard() noexcept {}
            virtual void commit() noexcept {}
        };

        class EphemeralSpool final : public SpoolBase {
        public:
            EphemeralSpool(uint64_t size, const std::shared_ptr<TransferBudget>& budget)
                : size_{size}, reservation_{budget->reserve(TransferBudget::Resource::SPOOL, size)} {
                auto directory = baseSpoolDirectory(budget->options);
                std::filesystem::create_directories(directory);
#ifdef _WIN32
                std::array<uint8_t, 16> random{};
                crypto::secureRandom(random);
                const auto path = directory / ("akk-transfer-" + idHex(random));
                file_ = ::CreateFileW(path.c_str(), GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_DELETE, nullptr,
                    CREATE_NEW, FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE, nullptr);
                if (file_ == INVALID_HANDLE_VALUE) { throw std::runtime_error("replication: cannot create transfer spool"); }
#else
                auto name = (directory / "akk-transfer-XXXXXX").string();
                name.push_back('\0');
                file_ = ::mkstemp(name.data());
                if (file_ < 0) { throw std::runtime_error("replication: cannot create transfer spool"); }
                if (::unlink(name.data()) != 0) { ::close(file_); file_ = -1; throw std::runtime_error("replication: cannot unlink transfer spool"); }
#endif
            }
            ~EphemeralSpool() override { close(); }
            void append(std::span<const uint8_t> bytes) override { write(bytes); }
            std::span<const uint8_t> finish() override { return map(); }
            uint64_t written() const noexcept override { return written_; }
        private:
            void write(std::span<const uint8_t> bytes) {
                if (bytes.size() > size_ - written_) { throw std::runtime_error("replication: oversized transfer"); }
                while (!bytes.empty()) {
#ifdef _WIN32
                    DWORD count = 0;
                    const auto request = static_cast<DWORD>(std::min<size_t>(bytes.size(), 1u << 20));
                    if (!::WriteFile(file_, bytes.data(), request, &count, nullptr) || count == 0) { throw std::runtime_error("replication: spool write failed"); }
#else
                    const auto count = ::write(file_, bytes.data(), bytes.size());
                    if (count < 0 && errno == EINTR) { continue; }
                    if (count <= 0) { throw std::runtime_error("replication: spool write failed"); }
#endif
                    written_ += static_cast<uint64_t>(count);
                    bytes = bytes.subspan(static_cast<size_t>(count));
                }
            }
            std::span<const uint8_t> map() {
                if (written_ != size_ || size_ == 0) { throw std::runtime_error("replication: incomplete transfer"); }
#ifdef _WIN32
                mapping_ = ::CreateFileMappingW(file_, nullptr, PAGE_READONLY, 0, 0, nullptr);
                if (!mapping_) { throw std::runtime_error("replication: spool mapping failed"); }
                view_ = ::MapViewOfFile(mapping_, FILE_MAP_READ, 0, 0, static_cast<SIZE_T>(size_));
                if (!view_) { throw std::runtime_error("replication: spool view failed"); }
#else
                view_ = ::mmap(nullptr, static_cast<size_t>(size_), PROT_READ, MAP_PRIVATE, file_, 0);
                if (view_ == MAP_FAILED) { view_ = nullptr; throw std::runtime_error("replication: spool mapping failed"); }
#endif
                return {static_cast<const uint8_t*>(view_), static_cast<size_t>(size_)};
            }
            void close() noexcept {
#ifdef _WIN32
                if (view_) { ::UnmapViewOfFile(view_); }
                if (mapping_) { ::CloseHandle(mapping_); }
                if (file_ != INVALID_HANDLE_VALUE) { ::CloseHandle(file_); }
#else
                if (view_) { ::munmap(view_, static_cast<size_t>(size_)); }
                if (file_ >= 0) { ::close(file_); }
#endif
            }
            uint64_t size_, written_ = 0;
            std::shared_ptr<void> reservation_;
            void* view_ = nullptr;
#ifdef _WIN32
            HANDLE file_ = INVALID_HANDLE_VALUE, mapping_ = nullptr;
#else
            int file_ = -1;
#endif
        };

        struct HeapPayload {
            std::vector<uint8_t> bytes;
            std::shared_ptr<void> reservation;
        };
    }

    class PersistentSpool final : public SpoolBase {
    public:
        PersistentSpool(uint64_t size, const TransferId& id, std::string scope, const std::shared_ptr<TransferBudget>& budget)
            : size_{size}, budget_{budget}, path_{resumeDirectory(budget->options) / std::move(scope) / (idHex(id) + ".part")} {
            std::filesystem::create_directories(path_.parent_path());
#ifndef _WIN32
            std::error_code permissionError;
            std::filesystem::permissions(path_.parent_path(), std::filesystem::perms::owner_all,
                std::filesystem::perm_options::replace, permissionError);
#endif
            const auto key = path_.string();
            {
                std::lock_guard lock{budget_->mutex_};
                if (budget_->claimedPaths_.contains(key)) { throw std::runtime_error("replication: duplicate active transfer"); }
                struct Candidate { std::filesystem::path path; std::filesystem::file_time_type modified; };
                std::vector<Candidate> candidates;
                std::error_code cleanupError;
                const auto now = std::filesystem::file_time_type::clock::now();
                for (std::filesystem::recursive_directory_iterator it{resumeDirectory(budget_->options), cleanupError};
                    !cleanupError && it != std::filesystem::recursive_directory_iterator{}; it.increment(cleanupError)) {
                    if (!it->is_regular_file(cleanupError) || !isPartial(it->path()) ||
                        budget_->claimedPaths_.contains(it->path().string())) { cleanupError.clear(); continue; }
                    const auto modified = it->last_write_time(cleanupError);
                    if (cleanupError) { cleanupError.clear(); continue; }
                    if (now - modified > std::chrono::milliseconds{budget_->options.resumeRetentionMs}) {
                        std::filesystem::remove(it->path(), cleanupError);
                        if (!cleanupError) { ++budget_->discardedPartials_; }
                        cleanupError.clear();
                    } else { candidates.push_back({it->path(), modified}); }
                }
                std::ranges::sort(candidates, {}, &Candidate::modified);
                while (candidates.size() >= budget_->options.maxResumeTransfers &&
                    !(candidates.size() == 1 && candidates.front().path == path_)) {
                    auto oldest = candidates.begin();
                    if (oldest->path == path_ && candidates.size() > 1) { ++oldest; }
                    std::filesystem::remove(oldest->path, cleanupError);
                    if (!cleanupError) { ++budget_->discardedPartials_; }
                    cleanupError.clear();
                    candidates.erase(oldest);
                }
                if (!std::filesystem::exists(path_, cleanupError) && partialCount(*budget_) >= budget_->options.maxResumeTransfers) {
                    throw std::runtime_error("replication: retained transfer count limit exceeded");
                }
                written_ = fileSize(path_);
                if (written_ > size_) {
                    std::error_code ec;
                    std::filesystem::remove(path_, ec);
                    written_ = 0;
                    ++budget_->discardedPartials_;
                }
                const auto onDisk = diskUsage(*budget_);
                const auto remaining = size_ - written_;
                if (onDisk > budget_->options.maxSpoolBytes || budget_->spool_ > budget_->options.maxSpoolBytes - onDisk ||
                    budget_->persistentRemaining_ > budget_->options.maxSpoolBytes - onDisk - budget_->spool_ ||
                    remaining > budget_->options.maxSpoolBytes - onDisk - budget_->spool_ - budget_->persistentRemaining_) {
                    throw std::runtime_error("replication: transfer spool limit exceeded");
                }
                budget_->persistentRemaining_ += remaining;
                reservedRemaining_ = remaining;
                budget_->claimedPaths_.insert(key);
                ++budget_->resumeAttempts_;
                if (written_ != 0) { ++budget_->resumedTransfers_; budget_->resumedBytes_ += written_; }
            }
            try { open(); }
            catch (...) { close(); releaseClaim(); throw; }
        }
        ~PersistentSpool() override {
            close();
            std::lock_guard lock{budget_->mutex_};
            if (removeOnClose_) {
                std::error_code ec;
                std::filesystem::remove(path_, ec);
            }
            budget_->persistentRemaining_ -= reservedRemaining_;
            budget_->claimedPaths_.erase(path_.string());
        }
        void append(std::span<const uint8_t> bytes) override {
            if (bytes.size() > size_ - written_) { throw std::runtime_error("replication: oversized transfer"); }
            while (!bytes.empty()) {
#ifdef _WIN32
                DWORD count = 0;
                const auto request = static_cast<DWORD>(std::min<size_t>(bytes.size(), 1u << 20));
                if (!::WriteFile(file_, bytes.data(), request, &count, nullptr) || count == 0) { throw std::runtime_error("replication: spool write failed"); }
#else
                const auto count = ::write(file_, bytes.data(), bytes.size());
                if (count < 0 && errno == EINTR) { continue; }
                if (count <= 0) { throw std::runtime_error("replication: spool write failed"); }
#endif
                written_ += static_cast<uint64_t>(count);
                {
                    std::lock_guard lock{budget_->mutex_};
                    const auto consumed = std::min<uint64_t>(reservedRemaining_, static_cast<uint64_t>(count));
                    reservedRemaining_ -= consumed;
                    budget_->persistentRemaining_ -= consumed;
                }
                bytes = bytes.subspan(static_cast<size_t>(count));
            }
#ifdef _WIN32
            if (!::FlushFileBuffers(file_)) { throw std::runtime_error("replication: spool sync failed"); }
#else
            if (::fdatasync(file_) != 0) { throw std::runtime_error("replication: spool sync failed"); }
#endif
        }
        std::span<const uint8_t> finish() override {
            if (written_ != size_ || size_ == 0) { throw std::runtime_error("replication: incomplete transfer"); }
#ifdef _WIN32
            mapping_ = ::CreateFileMappingW(file_, nullptr, PAGE_READONLY, 0, 0, nullptr);
            if (!mapping_) { throw std::runtime_error("replication: spool mapping failed"); }
            view_ = ::MapViewOfFile(mapping_, FILE_MAP_READ, 0, 0, static_cast<SIZE_T>(size_));
            if (!view_) { throw std::runtime_error("replication: spool view failed"); }
#else
            view_ = ::mmap(nullptr, static_cast<size_t>(size_), PROT_READ, MAP_PRIVATE, file_, 0);
            if (view_ == MAP_FAILED) { view_ = nullptr; throw std::runtime_error("replication: spool mapping failed"); }
#endif
            return {static_cast<const uint8_t*>(view_), static_cast<size_t>(size_)};
        }
        uint64_t written() const noexcept override { return written_; }
        void discard() noexcept override {
            removeOnClose_ = true;
            std::lock_guard lock{budget_->mutex_};
            ++budget_->discardedPartials_;
        }
        void commit() noexcept override { removeOnClose_ = true; }
    private:
        void open() {
#ifdef _WIN32
            file_ = ::CreateFileW(path_.c_str(), GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ, nullptr, OPEN_ALWAYS,
                FILE_ATTRIBUTE_NORMAL, nullptr);
            if (file_ == INVALID_HANDLE_VALUE) { throw std::runtime_error("replication: cannot open persistent transfer spool"); }
            LARGE_INTEGER offset{};
            offset.QuadPart = static_cast<LONGLONG>(written_);
            LARGE_INTEGER actualSize{};
            if (!::GetFileSizeEx(file_, &actualSize) || static_cast<uint64_t>(actualSize.QuadPart) != written_ ||
                !::SetFilePointerEx(file_, offset, nullptr, FILE_BEGIN)) { throw std::runtime_error("replication: spool seek failed"); }
#else
            int flags = O_CREAT | O_RDWR;
#ifdef O_NOFOLLOW
            flags |= O_NOFOLLOW;
#endif
            file_ = ::open(path_.c_str(), flags, S_IRUSR | S_IWUSR);
            if (file_ < 0) { throw std::runtime_error("replication: cannot open persistent transfer spool"); }
            struct stat info{};
            if (::fstat(file_, &info) != 0 || !S_ISREG(info.st_mode) || static_cast<uint64_t>(info.st_size) != written_ ||
                ::lseek(file_, static_cast<off_t>(written_), SEEK_SET) < 0) {
                throw std::runtime_error("replication: invalid persistent transfer spool");
            }
#endif
        }
        void close() noexcept {
#ifdef _WIN32
            if (view_) { ::UnmapViewOfFile(view_); }
            if (mapping_) { ::CloseHandle(mapping_); }
            if (file_ != INVALID_HANDLE_VALUE) { ::CloseHandle(file_); }
#else
            if (view_) { ::munmap(view_, static_cast<size_t>(size_)); }
            if (file_ >= 0) { ::close(file_); }
#endif
        }
        void releaseClaim() noexcept {
            if (!budget_) { return; }
            std::lock_guard lock{budget_->mutex_};
            budget_->persistentRemaining_ -= reservedRemaining_;
            reservedRemaining_ = 0;
            budget_->claimedPaths_.erase(path_.string());
        }
        uint64_t size_, written_ = 0, reservedRemaining_ = 0;
        std::shared_ptr<TransferBudget> budget_;
        std::filesystem::path path_;
        bool removeOnClose_ = false;
        void* view_ = nullptr;
#ifdef _WIN32
        HANDLE file_ = INVALID_HANDLE_VALUE, mapping_ = nullptr;
#else
        int file_ = -1;
#endif
    };

    struct TransferBudget::Lease {
        std::shared_ptr<TransferBudget> budget;
        Resource resource;
        uint64_t amount;
        bool active = false;
        ~Lease() { if (active) { std::lock_guard lock{budget->mutex_}; budget->counter(resource) -= amount; } }
    };

    TransferBudget::TransferBudget(ReplicationTransferOptions value) : options{std::move(value)} {
        validateTransferOptions(options);
        if (!options.resumeEnabled) { return; }
        std::error_code ec;
        const auto directory = resumeDirectory(options);
        std::filesystem::create_directories(directory, ec);
        struct Candidate { std::filesystem::path path; std::filesystem::file_time_type modified; };
        std::vector<Candidate> candidates;
        const auto now = std::filesystem::file_time_type::clock::now();
        for (std::filesystem::recursive_directory_iterator it{directory, ec}; !ec && it != std::filesystem::recursive_directory_iterator{}; it.increment(ec)) {
            if (!it->is_regular_file(ec) || !isPartial(it->path())) { ec.clear(); continue; }
            const auto modified = it->last_write_time(ec);
            if (ec) { ec.clear(); continue; }
            if (now - modified > std::chrono::milliseconds{options.resumeRetentionMs}) {
                std::filesystem::remove(it->path(), ec);
                if (!ec) { ++discardedPartials_; }
                ec.clear();
            } else { candidates.push_back({it->path(), modified}); }
        }
        std::ranges::sort(candidates, {}, &Candidate::modified);
        while (candidates.size() > options.maxResumeTransfers) {
            std::filesystem::remove(candidates.front().path, ec);
            if (!ec) { ++discardedPartials_; }
            ec.clear();
            candidates.erase(candidates.begin());
        }
    }
    uint64_t& TransferBudget::counter(Resource resource) {
        return resource == Resource::MEMORY ? memory_ : resource == Resource::SPOOL ? spool_ : active_;
    }
    std::shared_ptr<void> TransferBudget::reserve(Resource resource, uint64_t amount) {
        auto lease = std::make_shared<Lease>();
        lease->budget = shared_from_this(); lease->resource = resource; lease->amount = amount;
        std::lock_guard lock{mutex_};
        auto& current = counter(resource);
        const auto limit = resource == Resource::MEMORY ? options.maxMemoryBytes :
                           resource == Resource::SPOOL ? options.maxSpoolBytes : options.maxConcurrentTransfers;
        const auto extra = resource == Resource::SPOOL ? persistentRemaining_ + diskUsage(*this) : 0;
        if (amount > limit || extra > limit - amount || current > limit - amount - extra) {
            throw std::runtime_error("replication: transfer resource limit exceeded");
        }
        current += amount; lease->active = true;
        return lease;
    }
    uint64_t TransferBudget::used(Resource resource) const {
        std::lock_guard lock{mutex_};
        if (resource == Resource::SPOOL) { return spool_ + persistentRemaining_ + diskUsage(*this); }
        return resource == Resource::MEMORY ? memory_ : active_;
    }
    TransferBudget::Stats TransferBudget::stats() const {
        std::lock_guard lock{mutex_};
        const auto partials = partialCount(*this);
        return {.resumeAttempts = resumeAttempts_, .resumedTransfers = resumedTransfers_, .resumedBytes = resumedBytes_,
            .discardedPartials = discardedPartials_,
            .retainedPartials = partials > claimedPaths_.size() ? partials - claimedPaths_.size() : 0};
    }

    void TransferSession::reset() { std::lock_guard lock{mutex_}; cancelled_ = false; expected_.reset(); ready_.reset(); }
    void TransferSession::cancel() { std::lock_guard lock{mutex_}; cancelled_ = true; expected_.reset(); ready_.reset(); cv_.notify_all(); }
    void TransferSession::setScope(uint64_t groupId, uint64_t localNodeId, uint64_t remoteNodeId) {
        std::lock_guard lock{mutex_};
        scope_ = std::to_string(groupId) + "-" + std::to_string(localNodeId) + "-from-" + std::to_string(remoteNodeId);
    }
    std::string TransferSession::scope() const { std::lock_guard lock{mutex_}; return scope_; }
    void TransferSession::prepare(const TransferId& id) { std::lock_guard lock{mutex_}; expected_ = id; ready_.reset(); }
    bool TransferSession::notifyReady(const TransferId& id, uint64_t offset) {
        std::lock_guard lock{mutex_};
        if (cancelled_ || !expected_ || *expected_ != id) { return false; }
        ready_ = std::pair{id, offset}; cv_.notify_all();
        return true;
    }
    std::optional<uint64_t> TransferSession::waitReady(const TransferId& id, uint32_t timeoutMs) {
        std::unique_lock lock{mutex_};
        if (!cv_.wait_for(lock, std::chrono::milliseconds{timeoutMs}, [&] { return cancelled_ || (ready_ && ready_->first == id); })) { return std::nullopt; }
        if (cancelled_ || !ready_) { return std::nullopt; }
        const auto offset = ready_->second;
        expected_.reset();
        ready_.reset();
        return offset;
    }

    void validateTransferOptions(const ReplicationTransferOptions& options) { options.validate(); }

    MessagePtr makeMessage(ReplMsgType type, std::span<const std::span<const uint8_t>> parts, const std::shared_ptr<TransferBudget>& budget) {
        size_t size = 0;
        for (const auto part : parts) {
            if (part.size() > ReplFrameHeader::MAX_PAYLOAD_SIZE - size) { throw std::length_error("replication: logical frame too large"); }
            size += part.size();
        }
        if (!transferable(type) && size > TRANSFER_FRAME_LIMIT) { throw std::length_error("replication: control frame too large"); }
        auto message = std::make_shared<TransferMessage>();
        message->type = type;
        if (transferable(type) && size > budget->options.thresholdBytes) {
            auto spool = std::make_shared<EphemeralSpool>(size, budget);
            for (const auto part : parts) { spool->append(part); }
            message->payload = spool->finish(); message->storage = std::move(spool);
        } else {
            auto heap = std::make_shared<HeapPayload>();
            heap->reservation = budget->reserve(TransferBudget::Resource::MEMORY, size);
            heap->bytes.reserve(size);
            for (const auto part : parts) { heap->bytes.insert(heap->bytes.end(), part.begin(), part.end()); }
            message->payload = heap->bytes; message->storage = std::move(heap);
        }
        message->crc = checksum(message->payload);
        return message;
    }
    MessagePtr messageFromWire(std::vector<uint8_t> wire, const std::shared_ptr<TransferBudget>& budget) {
        if (wire.size() < ReplFrameHeader::SIZE) { throw std::length_error("replication: invalid encoded frame"); }
        const std::array parts{std::span<const uint8_t>{wire}.subspan(ReplFrameHeader::SIZE)};
        return makeMessage(static_cast<ReplMsgType>(wire[4]), parts, budget);
    }
    bool sendMessage(const TransferMessage& message, const std::shared_ptr<TransferBudget>& budget, const SendFrame& send) {
        return sendMessage(message, budget, {}, send);
    }
    bool sendMessage(const TransferMessage& message, const std::shared_ptr<TransferBudget>& budget,
        const std::shared_ptr<TransferSession>& session, const SendFrame& send) {
        if (!transferable(message.type) || message.payload.size() <= budget->options.thresholdBytes) {
            if (message.payload.size() > TRANSFER_FRAME_LIMIT) { throw std::length_error("replication: physical frame too large"); }
            return sendEncoded(message.type, message.payload, budget, send);
        }
        auto active = budget->reserve(TransferBudget::Resource::ACTIVE, 1);
        const auto id = makeTransferId(message);
        auto beginMemory = budget->reserve(TransferBudget::Resource::MEMORY, TRANSFER_BEGIN_SIZE);
        std::vector<uint8_t> begin;
        begin.reserve(TRANSFER_BEGIN_SIZE);
        begin.push_back(static_cast<uint8_t>(message.type)); begin.push_back(message.flags);
        appendLe(begin, message.payload.size(), 8); appendLe(begin, message.crc, 4);
        begin.insert(begin.end(), id.begin(), id.end());
        if (session) { session->prepare(id); }
        if (!sendEncoded(ReplMsgType::TRANSFER_BEGIN, begin, budget, send)) { return false; }
        uint64_t offset = 0;
        if (session) {
            const auto ready = session->waitReady(id, budget->options.resumeHandshakeTimeoutMs);
            if (!ready || *ready > message.payload.size()) { return false; }
            offset = *ready;
        }
        while (offset < message.payload.size()) {
            const auto count = std::min<size_t>(budget->options.chunkBytes, message.payload.size() - static_cast<size_t>(offset));
            auto chunkMemory = budget->reserve(TransferBudget::Resource::MEMORY, 8 + count);
            std::vector<uint8_t> chunk;
            chunk.reserve(8 + count); appendLe(chunk, offset, 8);
            chunk.insert(chunk.end(), message.payload.begin() + static_cast<size_t>(offset),
                message.payload.begin() + static_cast<size_t>(offset) + count);
            if (!sendEncoded(ReplMsgType::TRANSFER_CHUNK, chunk, budget, send)) { return false; }
            offset += count;
        }
        auto endMemory = budget->reserve(TransferBudget::Resource::MEMORY, 8);
        std::vector<uint8_t> end;
        appendLe(end, message.payload.size(), 8);
        return sendEncoded(ReplMsgType::TRANSFER_END, end, budget, send);
    }
    MessagePtr receiveMessage(const std::shared_ptr<TransferBudget>& budget, const ReceiveFrame& receive) {
        return receiveMessage(budget, {}, receive, {});
    }
    MessagePtr receiveMessage(const std::shared_ptr<TransferBudget>& budget, const std::shared_ptr<TransferSession>& session,
        const ReceiveFrame& receive, const SendFrame& sendControl) {
        auto receiveNext = [&](DecodedFrame& out) {
            for (;;) {
                if (!receive(out)) { return false; }
                TransferId readyId{}; uint64_t readyOffset = 0;
                if (decodeReady(out, readyId, readyOffset)) {
                    if (!session) { return false; }
                    if (!session->notifyReady(readyId, readyOffset)) { return false; }
                    out = {};
                    continue;
                }
                return true;
            }
        };
        auto first = std::make_shared<DecodedFrame>();
        if (!receiveNext(*first)) { return {}; }
        auto result = std::make_shared<TransferMessage>();
        if (first->type != ReplMsgType::TRANSFER_BEGIN) {
            if (first->type == ReplMsgType::TRANSFER_CHUNK || first->type == ReplMsgType::TRANSFER_END) { return {}; }
            result->type = first->type; result->flags = first->flags; result->payload = first->payload; result->storage = std::move(first);
            return result;
        }
        if (first->flags != 0 || first->payload.size() != TRANSFER_BEGIN_SIZE) { return {}; }
        result->type = static_cast<ReplMsgType>(first->payload[0]); result->flags = first->payload[1];
        const uint64_t size = readLe(first->payload, 2, 8);
        const uint32_t expectedCrc = static_cast<uint32_t>(readLe(first->payload, 10, 4));
        TransferId id{};
        std::copy_n(first->payload.begin() + 14, id.size(), id.begin());
        if (!transferable(result->type) || size == 0 || size > ReplFrameHeader::MAX_PAYLOAD_SIZE || result->flags != 0) { return {}; }
        first.reset();
        auto active = budget->reserve(TransferBudget::Resource::ACTIVE, 1);
        std::shared_ptr<SpoolBase> spool;
        if (budget->options.resumeEnabled && session && sendControl) { spool = std::make_shared<PersistentSpool>(size, id, session->scope(), budget); }
        else { spool = std::make_shared<EphemeralSpool>(size, budget); }
        uint64_t offset = spool->written();
        if (session && sendControl) {
            auto readyMemory = budget->reserve(TransferBudget::Resource::MEMORY, TRANSFER_READY_SIZE);
            std::vector<uint8_t> ready;
            ready.insert(ready.end(), id.begin(), id.end()); appendLe(ready, offset, 8);
            if (!sendEncoded(ReplMsgType::TRANSFER_READY, ready, budget, sendControl)) { return {}; }
        }
        for (;;) {
            DecodedFrame frame;
            if (!receiveNext(frame)) { return {}; }
            if (frame.type == ReplMsgType::TRANSFER_END) {
                if (frame.flags != 0 || frame.payload.size() != 8 || readLe(frame.payload, 0, 8) != size || offset != size) {
                    spool->discard(); return {};
                }
                break;
            }
            if (frame.type != ReplMsgType::TRANSFER_CHUNK || frame.flags != 0 || frame.payload.size() <= 8 ||
                readLe(frame.payload, 0, 8) != offset || frame.payload.size() - 8 > size - offset) {
                spool->discard(); return {};
            }
            spool->append(std::span<const uint8_t>{frame.payload}.subspan(8));
            offset += frame.payload.size() - 8;
        }
        result->payload = spool->finish();
        if (checksum(result->payload) != expectedCrc) { spool->discard(); return {}; }
        TransferMessage identity;
        identity.type = result->type;
        identity.flags = result->flags;
        identity.crc = expectedCrc;
        identity.payload = result->payload;
        if (makeTransferId(identity) != id) { spool->discard(); return {}; }
        spool->commit();
        result->crc = expectedCrc; result->storage = std::move(spool);
        return result;
    }

    MessagePtr entryMessage(uint64_t seq, uint64_t source, ReplOpType op, uint8_t flags, std::span<const uint8_t> key,
        std::span<const uint8_t> value, const std::shared_ptr<TransferBudget>& budget) {
        if (key.size() > UINT32_MAX || value.size() > UINT32_MAX) { throw std::length_error("replication: oversized entry"); }
        std::vector<uint8_t> prefix;
        appendLe(prefix, seq, 8); appendLe(prefix, source, 8); prefix.push_back(static_cast<uint8_t>(op)); prefix.push_back(flags);
        appendLe(prefix, key.size(), 4); appendLe(prefix, value.size(), 4);
        const std::array<std::span<const uint8_t>, 3> parts{prefix, key, value};
        return makeMessage(ReplMsgType::ENTRY, parts, budget);
    }
    MessagePtr snapshotMessage(const ReplSnapshotEntry& entry, const std::shared_ptr<TransferBudget>& budget) {
        return snapshotMessage(entry.key, entry.value, budget);
    }
    MessagePtr snapshotMessage(std::span<const uint8_t> key, std::span<const uint8_t> value,
        const std::shared_ptr<TransferBudget>& budget) {
        if (key.size() > UINT32_MAX || value.size() > UINT32_MAX) { throw std::length_error("replication: oversized snapshot entry"); }
        std::vector<uint8_t> prefix; appendLe(prefix, key.size(), 4); appendLe(prefix, value.size(), 4);
        const std::array<std::span<const uint8_t>, 3> parts{prefix, key, value};
        return makeMessage(ReplMsgType::SNAPSHOT_ENTRY, parts, budget);
    }
    MessagePtr readRequestMessage(uint64_t id, uint64_t snapshotSeq, std::span<const uint8_t> key, const std::shared_ptr<TransferBudget>& budget) {
        if (key.size() > UINT32_MAX) { throw std::length_error("replication: oversized read key"); }
        std::vector<uint8_t> prefix; appendLe(prefix, id, 8); appendLe(prefix, snapshotSeq, 8); appendLe(prefix, key.size(), 4);
        const std::array<std::span<const uint8_t>, 2> parts{prefix, key};
        return makeMessage(ReplMsgType::READ_REQUEST, parts, budget);
    }
    MessagePtr readResponseMessage(const ReadResponse& response, const std::shared_ptr<TransferBudget>& budget) {
        std::vector<uint8_t> prefix; appendLe(prefix, response.requestId, 8);
        prefix.push_back(static_cast<uint8_t>(response.status)); prefix.push_back(response.recordFlags);
        appendLe(prefix, response.seq, 8); appendLe(prefix, response.value.size(), 4);
        const std::array<std::span<const uint8_t>, 2> parts{prefix, response.value};
        return makeMessage(ReplMsgType::READ_RESPONSE, parts, budget);
    }
    MessagePtr blobMessage(uint64_t seq, uint64_t id, std::span<const uint8_t> value, const std::shared_ptr<TransferBudget>& budget) {
        std::vector<uint8_t> prefix; appendLe(prefix, seq, 8); appendLe(prefix, id, 8); appendLe(prefix, value.size(), 8);
        const std::array<std::span<const uint8_t>, 2> parts{prefix, value};
        return makeMessage(ReplMsgType::BLOB_PUT, parts, budget);
    }
    bool entryView(std::span<const uint8_t> payload, EntryView& entry) {
        if (payload.size() < 26) { return false; }
        entry.seq = readLe(payload, 0, 8); entry.sourceNodeId = readLe(payload, 8, 8);
        entry.op = static_cast<ReplOpType>(payload[16]); entry.recordFlags = payload[17];
        const auto keySize = readLe(payload, 18, 4), valueSize = readLe(payload, 22, 4);
        if (keySize > payload.size() - 26 || valueSize != payload.size() - 26 - keySize ||
            (entry.op != ReplOpType::PUT && entry.op != ReplOpType::REMOVE)) { return false; }
        entry.key = payload.subspan(26, static_cast<size_t>(keySize)); entry.value = payload.subspan(26 + static_cast<size_t>(keySize));
        return true;
    }
    bool snapshotView(std::span<const uint8_t> payload, std::span<const uint8_t>& key, std::span<const uint8_t>& value) {
        if (payload.size() < 8) { return false; }
        const auto keySize = readLe(payload, 0, 4), valueSize = readLe(payload, 4, 4);
        if (keySize > payload.size() - 8 || valueSize != payload.size() - 8 - keySize) { return false; }
        key = payload.subspan(8, static_cast<size_t>(keySize)); value = payload.subspan(8 + static_cast<size_t>(keySize));
        return true;
    }
}

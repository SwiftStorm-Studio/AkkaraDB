/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
#pragma once

#include "akk/engine/cluster/detail/ReplicationTransfer.hpp"
#include "akk/crypto/Random.hpp"
#include <algorithm>
#include <cstdio>
#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace akkaradb::engine::cluster::detail {
    // One anonymous file per incoming owner snapshot. No engine mutation or
    // peer watermark is published until the complete snapshot has arrived.
    class PartitionSnapshotSpool {
    public:
        explicit PartitionSnapshotSpool(std::shared_ptr<TransferBudget> budget) : budget_{std::move(budget)} {
            const auto directory = budget_->options.spoolDirectory.empty()
                ? std::filesystem::temp_directory_path() : budget_->options.spoolDirectory;
            std::filesystem::create_directories(directory);
#ifdef _WIN32
            std::array<uint8_t, 16> nonce{}; crypto::secureRandom(nonce);
            std::wstring name = L"akk-partition-";
            for (const auto byte : nonce) {
                constexpr wchar_t hex[] = L"0123456789abcdef";
                name += hex[byte >> 4]; name += hex[byte & 15];
            }
            const auto handle = ::CreateFileW((directory / name).c_str(), GENERIC_READ | GENERIC_WRITE, 0, nullptr,
                CREATE_NEW, FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE, nullptr);
            if (handle == INVALID_HANDLE_VALUE) { throw std::runtime_error("Partition snapshot: cannot create spool"); }
            const int descriptor = ::_open_osfhandle(reinterpret_cast<intptr_t>(handle), _O_BINARY | _O_RDWR);
            if (descriptor < 0) { ::CloseHandle(handle); throw std::runtime_error("Partition snapshot: cannot open spool"); }
            file_ = ::_fdopen(descriptor, "w+b");
            if (!file_) { ::_close(descriptor); }
#else
            auto name = (directory / "akk-partition-XXXXXX").string();
            const int descriptor = ::mkstemp(name.data());
            if (descriptor < 0) { throw std::runtime_error("Partition snapshot: cannot create spool"); }
            if (::unlink(name.c_str()) != 0 || ::fcntl(descriptor, F_SETFD, FD_CLOEXEC) < 0) {
                ::close(descriptor); throw std::runtime_error("Partition snapshot: cannot protect spool");
            }
            file_ = ::fdopen(descriptor, "w+b");
            if (!file_) { ::close(descriptor); }
#endif
            if (!file_) { throw std::runtime_error("Partition snapshot: cannot create spool"); }
        }
        ~PartitionSnapshotSpool() { if (file_) { std::fclose(file_); } }
        void begin(std::span<const uint8_t> key, uint64_t size, uint32_t crc) {
            if (active_ || key.size() > UINT32_MAX || size > UINT64_MAX - 20 - key.size()) {
                throw std::runtime_error("Partition snapshot: invalid entry");
            }
            const uint64_t recordBytes = 20 + key.size() + size;
            if (recordBytes > UINT64_MAX / 2) { throw std::runtime_error("Partition snapshot: entry too large"); }
            // Receive spool and engine transaction staging coexist during install.
            leases_.push_back(budget_->reserve(TransferBudget::Resource::SPOOL, 2 * recordBytes));
            // Covers reservation handles and the engine's duplicate-key index.
            leases_.push_back(budget_->reserve(TransferBudget::Resource::MEMORY, key.size() + 128));
            integer(key.size(), 8); integer(size, 8); integer(crc, 4); write(key);
            active_ = true; size_ = size; offset_ = 0;
        }
        void append(uint64_t offset, std::span<const uint8_t> bytes) {
            if (!active_ || offset != offset_ || bytes.size() > size_ - offset_) {
                throw std::runtime_error("Partition snapshot: invalid chunk");
            }
            write(bytes);
            offset_ += bytes.size();
        }
        void finish() {
            if (!active_ || offset_ != size_) { throw std::runtime_error("Partition snapshot: corrupt entry"); }
            active_ = false; ++count_;
        }
        bool replay(const SnapshotEntryVisitor& visitor, uint64_t count) {
            if (active_ || count != count_ || std::fflush(file_) != 0) { throw std::runtime_error("Partition snapshot: incomplete snapshot"); }
#ifdef _WIN32
            const auto seek = ::_fseeki64(file_, 0, SEEK_SET);
#else
            const auto seek = ::fseeko(file_, 0, SEEK_SET);
#endif
            if (seek != 0) { throw std::runtime_error("Partition snapshot: spool seek failed"); }
            const auto chunkSize = budget_->options.chunkBytes;
            auto chunkLease = budget_->reserve(TransferBudget::Resource::MEMORY, chunkSize);
            std::vector<uint8_t> buffer(chunkSize);
            for (uint64_t i = 0; i < count_; ++i) {
                const auto keySize = integer(8), valueSize = integer(8), crc = integer(4);
                if (keySize > UINT32_MAX) { throw std::runtime_error("Partition snapshot: invalid key size"); }
                auto keyLease = budget_->reserve(TransferBudget::Resource::MEMORY, keySize);
                std::vector<uint8_t> key(static_cast<size_t>(keySize)); read(key);
                if (!visitor.beginEntry(key, valueSize, static_cast<uint32_t>(crc))) { return false; }
                for (uint64_t offset = 0; offset < valueSize;) {
                    const auto chunk = std::span<uint8_t>{buffer}.first(static_cast<size_t>(std::min<uint64_t>(chunkSize, valueSize - offset)));
                    read(chunk);
                    if (!visitor.appendValueChunk(offset, chunk)) { return false; }
                    offset += chunk.size();
                }
                if (!visitor.finishEntry()) { return false; }
            }
            return true;
        }
    private:
        void write(std::span<const uint8_t> bytes) {
            if (!bytes.empty() && std::fwrite(bytes.data(), 1, bytes.size(), file_) != bytes.size()) {
                throw std::runtime_error("Partition snapshot: spool write failed");
            }
        }
        void read(std::span<uint8_t> bytes) {
            if (!bytes.empty() && std::fread(bytes.data(), 1, bytes.size(), file_) != bytes.size()) {
                throw std::runtime_error("Partition snapshot: spool read failed");
            }
        }
        void integer(uint64_t value, size_t width) {
            std::array<uint8_t, 8> bytes{};
            for (size_t i = 0; i < width; ++i) { bytes[i] = static_cast<uint8_t>(value >> (8 * i)); }
            write(std::span{bytes}.first(width));
        }
        uint64_t integer(size_t width) {
            std::array<uint8_t, 8> bytes{}; read(std::span{bytes}.first(width));
            uint64_t value = 0;
            for (size_t i = 0; i < width; ++i) { value |= static_cast<uint64_t>(bytes[i]) << (8 * i); }
            return value;
        }
        std::shared_ptr<TransferBudget> budget_;
        std::FILE* file_ = nullptr;
        std::vector<std::shared_ptr<void>> leases_;
        uint64_t count_ = 0, size_ = 0, offset_ = 0;
        bool active_ = false;
    };
}

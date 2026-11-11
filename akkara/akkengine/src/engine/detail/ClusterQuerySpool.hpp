/*
 * AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0
 */
#pragma once

#include "akk/crypto/Random.hpp"
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>
#ifdef _WIN32
#include <Windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace akkaradb::engine::detail {
    // Anonymous/delete-on-close scratch storage. No persisted database format,
    // secret-bearing named leftovers, or thread-owned iterator locks survive a query RPC.
    class ClusterQuerySpool {
    public:
        ClusterQuerySpool(const std::filesystem::path& directory, std::atomic<uint64_t>& used, uint64_t limit)
            : used_{used}, limit_{limit}, directory_{directory} {}
    private:
        void createFile() {
            std::filesystem::create_directories(directory_);
#ifdef _WIN32
            std::array<uint8_t, 16> nonce{};
            crypto::secureRandom(nonce);
            std::wstring name = L"akk-query-";
            for (auto byte : nonce) {
                constexpr wchar_t hex[] = L"0123456789abcdef";
                name += hex[byte >> 4]; name += hex[byte & 15];
            }
            file_ = ::CreateFileW((directory_ / name).c_str(), GENERIC_READ | GENERIC_WRITE, 0, nullptr, CREATE_NEW,
                FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE, nullptr);
            if (file_ == INVALID_HANDLE_VALUE) { throw std::runtime_error("Cluster query: cannot create spool"); }
#else
            auto name = (directory_ / "akk-query-XXXXXX").string();
            file_ = ::mkstemp(name.data());
            if (file_ < 0) { throw std::runtime_error("Cluster query: cannot create spool"); }
            if (::unlink(name.c_str()) != 0) {
                ::close(file_); file_ = -1; throw std::runtime_error("Cluster query: cannot unlink spool");
            }
            if (::fcntl(file_, F_SETFD, FD_CLOEXEC) < 0) {
                ::close(file_); file_ = -1; throw std::runtime_error("Cluster query: cannot protect spool descriptor");
            }
#endif
        }
    public:
        ~ClusterQuerySpool() {
#ifdef _WIN32
            if (file_ != INVALID_HANDLE_VALUE) { ::CloseHandle(file_); }
#else
            if (file_ >= 0) { ::close(file_); }
#endif
            used_.fetch_sub(size_);
        }
        ClusterQuerySpool(const ClusterQuerySpool&) = delete;
        ClusterQuerySpool& operator=(const ClusterQuerySpool&) = delete;
        [[nodiscard]] uint64_t size() const noexcept { return size_; }
        void append(std::span<const uint8_t> bytes) {
            uint64_t prior = used_.load();
            do {
                if (prior > limit_ || bytes.size() > limit_ - prior) {
                    throw std::runtime_error("Cluster query: spool byte limit exceeded");
                }
            } while (!used_.compare_exchange_weak(prior, prior + bytes.size()));
            // Reserve even on a short write; destruction releases the full reservation.
            size_ += bytes.size();
            while (!bytes.empty()) {
                const auto count = std::min(bytes.size(), BUFFER_BYTES - buffer_.size());
                buffer_.insert(buffer_.end(), bytes.begin(), bytes.begin() + static_cast<std::ptrdiff_t>(count));
                bytes = bytes.subspan(count);
                if (buffer_.size() == BUFFER_BYTES && !bytes.empty()) { flush(); }
            }
        }
    private:
        void flush() {
            if (buffer_.empty()) { return; }
            if (!spilled_) { createFile(); spilled_ = true; }
#ifdef _WIN32
            LARGE_INTEGER position; position.QuadPart = static_cast<LONGLONG>(fileBytes_);
            if (!::SetFilePointerEx(file_, position, nullptr, FILE_BEGIN)) { throw std::runtime_error("Cluster query: seek failed"); }
#endif
            const auto bytes = std::span<const uint8_t>{buffer_};
            size_t offset = 0;
            while (offset < bytes.size()) {
                const auto count = std::min<size_t>(bytes.size() - offset, 1024 * 1024);
#ifdef _WIN32
                DWORD written = 0;
                if (!::WriteFile(file_, bytes.data() + offset, static_cast<DWORD>(count), &written, nullptr) || written == 0) {
                    throw std::runtime_error("Cluster query: spool write failed");
                }
#else
                const auto written = ::write(file_, bytes.data() + offset, count);
                if (written < 0 && errno == EINTR) { continue; }
                if (written <= 0) { throw std::runtime_error("Cluster query: spool write failed"); }
#endif
                offset += static_cast<size_t>(written);
            }
            fileBytes_ += buffer_.size();
            buffer_.clear();
        }
    public:
        [[nodiscard]] std::vector<uint8_t> read(uint64_t offset, size_t count) {
            if (offset > size_ || count > size_ - offset) { throw std::runtime_error("Cluster query: invalid spool offset"); }
            if (!spilled_) {
                return {buffer_.begin() + static_cast<std::ptrdiff_t>(offset),
                    buffer_.begin() + static_cast<std::ptrdiff_t>(offset + count)};
            }
            flush();
#ifdef _WIN32
            LARGE_INTEGER position; position.QuadPart = static_cast<LONGLONG>(offset);
            if (!::SetFilePointerEx(file_, position, nullptr, FILE_BEGIN)) { throw std::runtime_error("Cluster query: seek failed"); }
#endif
            std::vector<uint8_t> bytes(count);
            size_t done = 0;
            while (done < count) {
#ifdef _WIN32
                DWORD received = 0;
                if (!::ReadFile(file_, bytes.data() + done, static_cast<DWORD>(count - done), &received, nullptr) || received == 0) {
                    throw std::runtime_error("Cluster query: spool read failed");
                }
#else
                const auto received = ::pread(file_, bytes.data() + done, count - done, static_cast<off_t>(offset + done));
                if (received < 0 && errno == EINTR) { continue; }
                if (received <= 0) { throw std::runtime_error("Cluster query: spool read failed"); }
#endif
                done += static_cast<size_t>(received);
            }
            return bytes;
        }
    private:
        std::atomic<uint64_t>& used_;
        uint64_t limit_, size_ = 0;
        static constexpr size_t BUFFER_BYTES = 64 * 1024;
        std::filesystem::path directory_;
        std::vector<uint8_t> buffer_;
        bool spilled_ = false;
        uint64_t fileBytes_ = 0;
#ifdef _WIN32
        HANDLE file_ = INVALID_HANDLE_VALUE;
#else
        int file_ = -1;
#endif
    };
}

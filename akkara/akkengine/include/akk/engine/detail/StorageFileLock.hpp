/*
 * AkkaraDB
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
#pragma once

#include <filesystem>
#include <stdexcept>
#include <string>

#ifdef _WIN32
#include <windows.h>
#else
#include <cerrno>
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>
#endif

namespace akkaradb::engine::detail {
    // Keep the companion file in place: unlinking a locked inode would let a
    // second opener lock a different inode at the same path.
    class StorageFileLock {
        public:
            explicit StorageFileLock(const std::filesystem::path& path) {
                #ifdef _WIN32
                handle_ = ::CreateFileW(path.c_str(), GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ,
                    nullptr, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
                if (handle_ == INVALID_HANDLE_VALUE) {
                    throw std::runtime_error("Storage lock: cannot acquire " + path.string() + " error=" + std::to_string(::GetLastError()));
                }
                #else
                handle_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
                if (handle_ < 0) { throw std::runtime_error("Storage lock: cannot open " + path.string() + " errno=" + std::to_string(errno)); }
                int result;
                do { result = ::flock(handle_, LOCK_EX | LOCK_NB); } while (result != 0 && errno == EINTR);
                if (result != 0) {
                    const int saved = errno;
                    ::close(handle_);
                    throw std::runtime_error("Storage lock: cannot acquire " + path.string() + " errno=" + std::to_string(saved));
                }
                #endif
            }

            ~StorageFileLock() {
                #ifdef _WIN32
                ::CloseHandle(handle_);
                #else
                ::close(handle_);
                #endif
            }

            StorageFileLock(const StorageFileLock&) = delete;
            StorageFileLock& operator=(const StorageFileLock&) = delete;

        private:
            #ifdef _WIN32
            HANDLE handle_ = INVALID_HANDLE_VALUE;
            #else
            int handle_ = -1;
            #endif
    };
}

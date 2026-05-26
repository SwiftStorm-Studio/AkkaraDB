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

// akkengine/src/engine/cluster/ClusterManager.cpp
#include "akk/engine/cluster/ClusterManager.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <netdb.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace akkaradb::engine::cluster {
    namespace {
        #ifdef _WIN32
        using LockHandle = HANDLE; inline const LockHandle INVALID_LOCK = INVALID_HANDLE_VALUE; void net_init() {
            static std::once_flag once;
            std::call_once(
                once,
                [] {
                    WSADATA wsa{};
                    ::WSAStartup(MAKEWORD(2, 2), &wsa);
                }
            );
        } LockHandle try_acquire_lock(const std::filesystem::path& path) {
            auto h = ::CreateFileW(path.c_str(), GENERIC_READ | GENERIC_WRITE, 0, nullptr, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
            if (h == INVALID_HANDLE_VALUE) { return INVALID_LOCK; }
            OVERLAPPED ov{};
            if (!::LockFileEx(h, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &ov)) {
                ::CloseHandle(h);
                return INVALID_LOCK;
            }
            return h;
        } void release_lock(LockHandle h) noexcept {
            if (h != INVALID_LOCK) {
                OVERLAPPED ov{};
                ::UnlockFileEx(h, 0, 1, 0, &ov);
                ::CloseHandle(h);
            }
        } bool write_lock_file(LockHandle h, const NodeInfo& self) {
            const auto host_len = static_cast<uint16_t>(self.host.size());
            std::vector<uint8_t> bytes(12 + host_len);
            for (size_t i = 0; i < 8; ++i) { bytes[i] = static_cast<uint8_t>(self.node_id >> (8 * i)); }
            bytes[8] = static_cast<uint8_t>(self.repl_port);
            bytes[9] = static_cast<uint8_t>(self.repl_port >> 8);
            bytes[10] = static_cast<uint8_t>(host_len);
            bytes[11] = static_cast<uint8_t>(host_len >> 8);
            std::memcpy(bytes.data() + 12, self.host.data(), host_len);
            ::SetFilePointer(h, 0, nullptr, FILE_BEGIN);
            ::SetEndOfFile(h);
            DWORD written = 0;
            return ::WriteFile(h, bytes.data(), static_cast<DWORD>(bytes.size()), &written, nullptr) != 0 && written == bytes.size();
        } bool read_lock_file(const std::filesystem::path& path, uint64_t& node_id, uint16_t& repl_port, std::string& host) {
            HANDLE h = ::CreateFileW(path.c_str(), GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
            if (h == INVALID_HANDLE_VALUE) { return false; }
            uint8_t fixed[12]{};
            DWORD read = 0;
            if (!::ReadFile(h, fixed, sizeof(fixed), &read, nullptr) || read != sizeof(fixed)) {
                ::CloseHandle(h);
                return false;
            }
            node_id = 0;
            for (size_t i = 0; i < 8; ++i) { node_id |= static_cast<uint64_t>(fixed[i]) << (8 * i); }
            repl_port = static_cast<uint16_t>(fixed[8]) | static_cast<uint16_t>(fixed[9] << 8);
            const uint16_t host_len = static_cast<uint16_t>(fixed[10]) | static_cast<uint16_t>(fixed[11] << 8);
            host.resize(host_len);
            if (host_len > 0 && (!::ReadFile(h, host.data(), host_len, &read, nullptr) || read != host_len)) {
                ::CloseHandle(h);
                return false;
            }
            ::CloseHandle(h);
            return true;
        } bool tcp_reachable(const std::string& host, uint16_t port) noexcept {
            net_init();
            addrinfo hints{};
            hints.ai_family = AF_INET;
            hints.ai_socktype = SOCK_STREAM;
            addrinfo* result = nullptr;
            const std::string port_s = std::to_string(port);
            if (::getaddrinfo(host.c_str(), port_s.c_str(), &hints, &result) != 0) { return false; }
            SOCKET sock = ::socket(result->ai_family, result->ai_socktype, result->ai_protocol);
            if (sock == INVALID_SOCKET) {
                ::freeaddrinfo(result);
                return false;
            }
            const bool ok = ::connect(sock, result->ai_addr, static_cast<int>(result->ai_addrlen)) == 0;
            ::closesocket(sock);
            ::freeaddrinfo(result);
            return ok;
        }
        #else
        using LockHandle = int;
        constexpr LockHandle INVALID_LOCK = -1;

        LockHandle try_acquire_lock(const std::filesystem::path& path) {
            int fd = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
            if (fd < 0) { return INVALID_LOCK; }
            if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
                ::close(fd);
                return INVALID_LOCK;
            }
            return fd;
        }

        void release_lock(LockHandle fd) noexcept {
            if (fd >= 0) {
                ::flock(fd, LOCK_UN);
                ::close(fd);
            }
        }

        bool write_lock_file(LockHandle fd, const NodeInfo& self) {
            const auto host_len = static_cast<uint16_t>(self.host.size());
            std::vector<uint8_t> bytes(12 + host_len);
            for (size_t i = 0; i < 8; ++i) { bytes[i] = static_cast<uint8_t>(self.node_id >> (8 * i)); }
            bytes[8] = static_cast<uint8_t>(self.repl_port);
            bytes[9] = static_cast<uint8_t>(self.repl_port >> 8);
            bytes[10] = static_cast<uint8_t>(host_len);
            bytes[11] = static_cast<uint8_t>(host_len >> 8);
            std::memcpy(bytes.data() + 12, self.host.data(), host_len);
            ::lseek(fd, 0, SEEK_SET);
            ::ftruncate(fd, 0);
            return ::write(fd, bytes.data(), bytes.size()) == static_cast<ssize_t>(bytes.size());
        }

        bool read_lock_file(const std::filesystem::path& path, uint64_t& node_id, uint16_t& repl_port, std::string& host) {
            int fd = ::open(path.c_str(), O_RDONLY);
            if (fd < 0) { return false; }
            uint8_t fixed[12]{};
            if (::read(fd, fixed, sizeof(fixed)) != static_cast<ssize_t>(sizeof(fixed))) {
                ::close(fd);
                return false;
            }
            node_id = 0;
            for (size_t i = 0; i < 8; ++i) { node_id |= static_cast<uint64_t>(fixed[i]) << (8 * i); }
            repl_port = static_cast<uint16_t>(fixed[8]) | static_cast<uint16_t>(fixed[9] << 8);
            const uint16_t host_len = static_cast<uint16_t>(fixed[10]) | static_cast<uint16_t>(fixed[11] << 8);
            host.resize(host_len);
            if (host_len > 0 && ::read(fd, host.data(), host_len) != host_len) {
                ::close(fd);
                return false;
            }
            ::close(fd);
            return true;
        }

        bool tcp_reachable(const std::string& host, uint16_t port) noexcept {
            addrinfo hints{};
            hints.ai_family = AF_INET;
            hints.ai_socktype = SOCK_STREAM;
            addrinfo* result = nullptr;
            const std::string port_s = std::to_string(port);
            if (::getaddrinfo(host.c_str(), port_s.c_str(), &hints, &result) != 0) { return false; }
            int sock = ::socket(result->ai_family, result->ai_socktype, result->ai_protocol);
            if (sock < 0) {
                ::freeaddrinfo(result);
                return false;
            }
            const bool ok = ::connect(sock, result->ai_addr, result->ai_addrlen) == 0;
            ::close(sock);
            ::freeaddrinfo(result);
            return ok;
        }
        #endif
    } // namespace

    class ClusterManager::Impl {
        public:
            Impl(std::filesystem::path db_dir, ClusterConfig config, uint64_t self_node_id)
                : db_dir_{std::move(db_dir)}, config_{std::move(config)}, self_node_id_{self_node_id} {
                config_.validate();
                if (const NodeInfo* self = config_.find_by_id(self_node_id_)) { self_ = *self; }
                else if (!config_.is_standalone()) { throw std::runtime_error("ClusterManager: self_node_id not found in cluster config"); }
            }

            ~Impl() { close(); }

            void set_role_change_callback(RoleChangeCallback callback) {
                std::lock_guard lock{callback_mutex_};
                callback_ = std::move(callback);
            }

            void start() {
                if (config_.is_standalone()) {
                    set_role(NodeRole::Standalone);
                    return;
                }
                if (running_.exchange(true)) { return; }
                std::filesystem::create_directories(db_dir_);
                elect_role();
                monitor_thread_ = std::thread([this] { monitor_loop(); });
            }

            void close() {
                running_.store(false);
                monitor_cv_.notify_all();
                if (monitor_thread_.joinable()) { monitor_thread_.join(); }
                release_lock(lock_handle_);
                lock_handle_ = INVALID_LOCK;
            }

            NodeRole role() const noexcept { return role_.load(); }
            uint64_t self_node_id() const noexcept { return self_node_id_; }
            bool is_standalone() const noexcept { return config_.is_standalone(); }

            std::string primary_host() const {
                std::lock_guard lock{primary_mutex_};
                return primary_host_;
            }

            uint16_t primary_repl_port() const {
                std::lock_guard lock{primary_mutex_};
                return primary_repl_port_;
            }

        private:
            void set_role(NodeRole role) {
                const NodeRole old = role_.exchange(role);
                if (old == role) { return; }
                RoleChangeCallback cb;
                {
                    std::lock_guard lock{callback_mutex_};
                    cb = callback_;
                }
                if (cb) { cb(role); }
            }

            void elect_role() {
                const auto lock_path = db_dir_ / "PRIMARY.lock";
                LockHandle h = INVALID_LOCK;
                if (self_.coordinator_eligible()) { h = try_acquire_lock(lock_path); }

                if (h != INVALID_LOCK) {
                    lock_handle_ = h;
                    if (!write_lock_file(h, self_)) { throw std::runtime_error("ClusterManager: failed to write PRIMARY.lock"); }
                    {
                        std::lock_guard lock{primary_mutex_};
                        primary_host_ = self_.host;
                        primary_repl_port_ = self_.repl_port;
                    }
                    set_role(NodeRole::Primary);
                    return;
                }

                uint64_t primary_id = 0;
                uint16_t primary_port = 0;
                std::string primary_host;
                if (read_lock_file(lock_path, primary_id, primary_port, primary_host)) {
                    std::lock_guard lock{primary_mutex_};
                    primary_host_ = std::move(primary_host);
                    primary_repl_port_ = primary_port;
                }
                set_role(NodeRole::Replica);
            }

            void monitor_loop() {
                constexpr auto interval = std::chrono::milliseconds(2000);
                constexpr int max_failures = 3;
                int failures = 0;

                while (running_.load()) {
                    std::unique_lock lock{monitor_mutex_};
                    monitor_cv_.wait_for(lock, interval, [this] { return !running_.load(); });
                    if (!running_.load()) { break; }
                    lock.unlock();

                    if (role() != NodeRole::Replica) {
                        failures = 0;
                        continue;
                    }

                    const std::string host = primary_host();
                    const uint16_t port = primary_repl_port();
                    if (!host.empty() && port != 0 && tcp_reachable(host, port)) {
                        failures = 0;
                        continue;
                    }

                    if (++failures >= max_failures) {
                        failures = 0;
                        release_lock(lock_handle_);
                        lock_handle_ = INVALID_LOCK;
                        elect_role();
                    }
                }
            }

            std::filesystem::path db_dir_;
            ClusterConfig config_;
            uint64_t self_node_id_;
            NodeInfo self_{};
            std::atomic<NodeRole> role_{NodeRole::Standalone};
            std::atomic<bool> running_{false};
            LockHandle lock_handle_{INVALID_LOCK};

            mutable std::mutex primary_mutex_;
            std::string primary_host_;
            uint16_t primary_repl_port_ = 0;

            std::thread monitor_thread_;
            std::mutex monitor_mutex_;
            std::condition_variable monitor_cv_;

            std::mutex callback_mutex_;
            RoleChangeCallback callback_;
    };

    std::unique_ptr<ClusterManager> ClusterManager::create(std::filesystem::path db_dir, ClusterConfig config, uint64_t self_node_id) {
        return std::unique_ptr<ClusterManager>(new ClusterManager(std::make_unique<Impl>(std::move(db_dir), std::move(config), self_node_id)));
    }

    ClusterManager::ClusterManager(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {}
    ClusterManager::~ClusterManager() = default;

    void ClusterManager::set_role_change_callback(RoleChangeCallback callback) { impl_->set_role_change_callback(std::move(callback)); }
    void ClusterManager::start() { impl_->start(); }
    void ClusterManager::close() { impl_->close(); }
    NodeRole ClusterManager::role() const noexcept { return impl_->role(); }
    uint64_t ClusterManager::self_node_id() const noexcept { return impl_->self_node_id(); }
    std::string ClusterManager::primary_host() const { return impl_->primary_host(); }
    uint16_t ClusterManager::primary_repl_port() const { return impl_->primary_repl_port(); }
    bool ClusterManager::is_standalone() const noexcept { return impl_->is_standalone(); }
} // namespace akkaradb::engine::cluster

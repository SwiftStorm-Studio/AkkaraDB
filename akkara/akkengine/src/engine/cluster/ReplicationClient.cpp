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

// akkengine/src/engine/cluster/ReplicationClient.cpp
#include "akk/engine/cluster/ReplicationClient.hpp"
#include "akk/net/tls/TlsStream.hpp"

#include <atomic>
#include <chrono>
#include <cstring>
#include <mutex>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace akkaradb::engine::cluster {
    namespace {
        #ifdef _WIN32
        using SocketHandle = SOCKET;
        constexpr SocketHandle INVALID_SOCKET_HANDLE = INVALID_SOCKET;
        void shutdown_socket(SocketHandle s) noexcept { if (s != INVALID_SOCKET_HANDLE) { ::shutdown(s, SD_BOTH); } }
        #else
        using SocketHandle = int; constexpr SocketHandle INVALID_SOCKET_HANDLE = -1; void shutdown_socket(SocketHandle s) noexcept {
            if (s >= 0) { ::shutdown(s, SHUT_RDWR); }
        }
        #endif

        void ensure_socket_runtime() {
            #ifdef _WIN32
            static std::once_flag once;
            std::call_once(
                once,
                [] {
                    WSADATA data{};
                    if (::WSAStartup(MAKEWORD(2, 2), &data) != 0) { throw std::runtime_error("ReplicationClient: WSAStartup failed"); }
                }
            );
            #endif
        }

        void close_socket(SocketHandle s) noexcept {
            if (s == INVALID_SOCKET_HANDLE) { return; }
            #ifdef _WIN32
            ::closesocket(s);
            #else
            ::close(s);
            #endif
        }

        bool send_all(SocketHandle s, const uint8_t* data, size_t size) {
            size_t sent = 0;
            while (sent < size) {
                #ifdef _WIN32
                const int n = ::send(s, reinterpret_cast<const char*>(data + sent), static_cast<int>(size - sent), 0);
                #else
                const ssize_t n = ::send(s, data + sent, size - sent, 0);
                #endif
                if (n <= 0) { return false; }
                sent += static_cast<size_t>(n);
            }
            return true;
        }

        bool recv_all(SocketHandle s, uint8_t* data, size_t size) {
            size_t received = 0;
            while (received < size) {
                #ifdef _WIN32
                const int n = ::recv(s, reinterpret_cast<char*>(data + received), static_cast<int>(size - received), 0);
                #else
                const ssize_t n = ::recv(s, data + received, size - received, 0);
                #endif
                if (n <= 0) { return false; }
                received += static_cast<size_t>(n);
            }
            return true;
        }

        bool send_all(net::TlsStream& stream, const uint8_t* data, size_t size) {
            size_t sent = 0;
            while (sent < size) { sent += stream.send(data + sent, size - sent); }
            return true;
        }

        bool recv_all(net::TlsStream& stream, uint8_t* data, size_t size) {
            size_t received = 0;
            while (received < size) { received += stream.recv(data + received, size - received); }
            return true;
        }

        bool recv_frame(SocketHandle s, DecodedFrame& out) {
            uint8_t header[ReplFrameHeader::SIZE];
            if (!recv_all(s, header, sizeof(header))) { return false; }

            const uint32_t payload_len = static_cast<uint32_t>(header[6]) | (static_cast<uint32_t>(header[7]) << 8) | (static_cast<uint32_t>
                (header[8]) << 16) | (static_cast<uint32_t>(header[9]) << 24);

            std::vector<uint8_t> wire(sizeof(header) + payload_len);
            std::memcpy(wire.data(), header, sizeof(header));
            if (payload_len > 0 && !recv_all(s, wire.data() + sizeof(header), payload_len)) { return false; }
            return decode_frame(wire, out);
        }

        bool recv_frame(net::TlsStream& stream, DecodedFrame& out) {
            uint8_t header[ReplFrameHeader::SIZE];
            try {
                if (!recv_all(stream, header, sizeof(header))) { return false; }

                const uint32_t payload_len = static_cast<uint32_t>(header[6]) | (static_cast<uint32_t>(header[7]) << 8) | (static_cast<
                    uint32_t>(header[8]) << 16) | (static_cast<uint32_t>(header[9]) << 24);

                std::vector<uint8_t> wire(sizeof(header) + payload_len);
                std::memcpy(wire.data(), header, sizeof(header));
                if (payload_len > 0 && !recv_all(stream, wire.data() + sizeof(header), payload_len)) { return false; }
                return decode_frame(wire, out);
            }
            catch (...) { return false; }
        }

        struct TlsConfigStorage {
            std::string cert_path;
            std::string key_path;
            std::string ca_path;
            net::TlsConfig config{};
        };

        TlsConfigStorage make_tls_config(const ClusterRuntimeOptions& options) {
            TlsConfigStorage storage;
            storage.cert_path = options.tls.cert_path.string();
            storage.key_path = options.tls.key_path.string();
            storage.ca_path = options.tls.ca_path.string();
            storage.config.cert_path = storage.cert_path.empty() ? nullptr : storage.cert_path.c_str();
            storage.config.key_path = storage.key_path.empty() ? nullptr : storage.key_path.c_str();
            storage.config.ca_path = storage.ca_path.empty() ? nullptr : storage.ca_path.c_str();
            storage.config.verify_peer = options.tls.verify_peer;
            return storage;
        }

        SocketHandle connect_to(const std::string& host, uint16_t port) {
            ensure_socket_runtime();

            addrinfo hints{};
            hints.ai_family = AF_UNSPEC;
            hints.ai_socktype = SOCK_STREAM;

            addrinfo* result = nullptr;
            const auto port_text = std::to_string(port);
            if (::getaddrinfo(host.c_str(), port_text.c_str(), &hints, &result) != 0) { return INVALID_SOCKET_HANDLE; }

            SocketHandle socket = INVALID_SOCKET_HANDLE;
            for (addrinfo* it = result; it != nullptr; it = it->ai_next) {
                socket = ::socket(it->ai_family, it->ai_socktype, it->ai_protocol);
                if (socket == INVALID_SOCKET_HANDLE) { continue; }
                if (::connect(socket, it->ai_addr, static_cast<int>(it->ai_addrlen)) == 0) { break; }
                close_socket(socket);
                socket = INVALID_SOCKET_HANDLE;
            }

            ::freeaddrinfo(result);
            return socket;
        }
    } // namespace

    class ReplicationClient::Impl {
        public:
            Impl(
                std::string primary_host,
                uint16_t primary_repl_port,
                uint64_t self_node_id,
                std::function<uint64_t()> get_last_seq,
                ClusterRuntimeOptions runtime_options
            )
                : primary_host_{std::move(primary_host)},
                  primary_repl_port_{primary_repl_port},
                  self_node_id_{self_node_id},
                  get_last_seq_{std::move(get_last_seq)},
                  runtime_options_{std::move(runtime_options)} {}

            ~Impl() { close(); }

            void set_apply_callback(ApplyCallback callback) {
                std::lock_guard lock{callback_mutex_};
                apply_callback_ = std::move(callback);
            }

            void set_blob_callback(BlobCallback callback) {
                std::lock_guard lock{callback_mutex_};
                blob_callback_ = std::move(callback);
            }

            void start() {
                if (running_.exchange(true)) { return; }
                worker_ = std::thread([this] { run(); });
            }

            void close() {
                running_ = false;
                {
                    std::lock_guard lock{socket_mutex_};
                    if (tls_) { tls_->shutdown(); }
                    shutdown_socket(socket_);
                    close_socket(socket_);
                    socket_ = INVALID_SOCKET_HANDLE;
                }
                if (worker_.joinable()) { worker_.join(); }
                {
                    std::lock_guard lock{socket_mutex_};
                    if (tls_) {
                        tls_->close();
                        tls_.reset();
                    }
                }
                connected_ = false;
            }

            bool connected() const noexcept { return connected_; }

        private:
            void run() {
                while (running_) {
                    SocketHandle socket = INVALID_SOCKET_HANDLE;
                    net::TlsStream* tls = nullptr;

                    if (runtime_options_.transport_mode == TransportMode::TLS) {
                        try {
                            auto storage = make_tls_config(runtime_options_);
                            auto stream = std::make_unique<net::TlsStream>();
                            stream->connect(primary_host_.c_str(), primary_repl_port_, storage.config);
                            tls = stream.get();
                            std::lock_guard lock{socket_mutex_};
                            tls_ = std::move(stream);
                        }
                        catch (...) {
                            std::this_thread::sleep_for(std::chrono::milliseconds(200));
                            continue;
                        }
                    }
                    else {
                        socket = connect_to(primary_host_, primary_repl_port_);
                        if (socket == INVALID_SOCKET_HANDLE) {
                            std::this_thread::sleep_for(std::chrono::milliseconds(200));
                            continue;
                        }

                        {
                            std::lock_guard lock{socket_mutex_};
                            socket_ = socket;
                        }
                    }

                    if (handshake(socket, tls)) {
                        connected_ = true;
                        receive_loop(socket, tls);
                    }

                    connected_ = false;
                    {
                        std::lock_guard lock{socket_mutex_};
                        if (tls_ && tls_.get() == tls) {
                            tls_->close();
                            tls_.reset();
                        }
                        if (socket_ == socket) { socket_ = INVALID_SOCKET_HANDLE; }
                    }
                    close_socket(socket);

                    if (running_) { std::this_thread::sleep_for(std::chrono::milliseconds(200)); }
                }
            }

            bool handshake(SocketHandle socket, net::TlsStream* tls) {
                const ClientHello hello{
                    .node_id = self_node_id_,
                    .last_seq = get_last_seq_ ? get_last_seq_() : 0,
                    .role = NodeRole::Replica,
                };
                const auto wire = encode_client_hello(hello);
                if (!send_to(socket, tls, wire.data(), wire.size())) { return false; }

                DecodedFrame frame;
                if (!recv_frame_from(socket, tls, frame) || frame.type != ReplMsgType::ServerHello) { return false; }
                ServerHello server_hello;
                return decode_server_hello(frame.payload, server_hello);
            }

            static bool send_to(SocketHandle socket, net::TlsStream* tls, const uint8_t* data, size_t size) {
                try { return tls != nullptr ? send_all(*tls, data, size) : send_all(socket, data, size); }
                catch (...) { return false; }
            }

            static bool recv_frame_from(SocketHandle socket, net::TlsStream* tls, DecodedFrame& frame) {
                return tls != nullptr ? recv_frame(*tls, frame) : recv_frame(socket, frame);
            }

            void receive_loop(SocketHandle socket, net::TlsStream* tls) {
                while (running_) {
                    DecodedFrame frame;
                    if (!recv_frame_from(socket, tls, frame)) { return; }

                    if (frame.type == ReplMsgType::Entry) {
                        ReplEntry entry;
                        if (!decode_entry(frame.payload, entry)) { return; }
                        ApplyCallback callback;
                        {
                            std::lock_guard lock{callback_mutex_};
                            callback = apply_callback_;
                        }
                        if (callback) { callback(entry.seq, entry.op, entry.key, entry.value, entry.record_flags, entry.source_node_id); }
                        const auto ack = encode_ack(ReplAck{.seq = entry.seq});
                        if (!send_to(socket, tls, ack.data(), ack.size())) { return; }
                    }
                    else if (frame.type == ReplMsgType::BlobPut) {
                        ReplBlob blob;
                        if (!decode_blob(frame.payload, blob)) { return; }
                        BlobCallback callback;
                        {
                            std::lock_guard lock{callback_mutex_};
                            callback = blob_callback_;
                        }
                        if (callback) { callback(blob.seq, blob.blob_id, blob.content); }
                    }
                    else if (frame.type != ReplMsgType::ReadResponse) { return; }
                }
            }

            std::string primary_host_;
            uint16_t primary_repl_port_;
            uint64_t self_node_id_;
            std::function<uint64_t()> get_last_seq_;
            ClusterRuntimeOptions runtime_options_;

            std::atomic<bool> running_{false};
            std::atomic<bool> connected_{false};
            std::thread worker_;

            mutable std::mutex socket_mutex_;
            SocketHandle socket_ = INVALID_SOCKET_HANDLE;
            std::unique_ptr<net::TlsStream> tls_;

            mutable std::mutex callback_mutex_;
            ApplyCallback apply_callback_;
            BlobCallback blob_callback_;
    };

    std::unique_ptr<ReplicationClient> ReplicationClient::create(
        std::string primary_host,
        uint16_t primary_repl_port,
        uint64_t self_node_id,
        std::function<uint64_t()> get_last_seq,
        ClusterRuntimeOptions runtime_options
    ) {
        return std::unique_ptr<ReplicationClient>(
            new ReplicationClient(
                std::make_unique<Impl>(
                    std::move(primary_host),
                    primary_repl_port,
                    self_node_id,
                    std::move(get_last_seq),
                    std::move(runtime_options)
                )
            )
        );
    }

    ReplicationClient::ReplicationClient(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {}

    ReplicationClient::~ReplicationClient() = default;

    void ReplicationClient::set_apply_callback(ApplyCallback callback) { impl_->set_apply_callback(std::move(callback)); }

    void ReplicationClient::set_blob_callback(BlobCallback callback) { impl_->set_blob_callback(std::move(callback)); }

    void ReplicationClient::start() { impl_->start(); }

    void ReplicationClient::close() { impl_->close(); }

    bool ReplicationClient::connected() const noexcept { return impl_->connected(); }
} // namespace akkaradb::engine::cluster

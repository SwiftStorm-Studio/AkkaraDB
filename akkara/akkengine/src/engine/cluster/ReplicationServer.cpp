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

// akkengine/src/engine/cluster/ReplicationServer.cpp
#include "akk/engine/cluster/ReplicationServer.hpp"
#include "akk/net/tls/TlsStream.hpp"

#include <atomic>
#include <chrono>
#include <cstring>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace akkaradb::engine::cluster {
    namespace {
        #ifdef _WIN32
        using socket_t = SOCKET; constexpr socket_t BAD_SOCKET = INVALID_SOCKET; void
        shutdown_socket(socket_t s) noexcept { if (s != BAD_SOCKET) { ::shutdown(s, SD_BOTH); } } void close_socket(socket_t s) noexcept {
            if (s != BAD_SOCKET) { ::closesocket(s); }
        } bool socket_ok(socket_t s) noexcept { return s != INVALID_SOCKET; } void net_init() {
            static std::once_flag once;
            std::call_once(
                once,
                [] {
                    WSADATA wsa{};
                    ::WSAStartup(MAKEWORD(2, 2), &wsa);
                }
            );
        }
        #else
        using socket_t = int;
        constexpr socket_t BAD_SOCKET = -1;
        void shutdown_socket(socket_t s) noexcept { if (s >= 0) { ::shutdown(s, SHUT_RDWR); } }
        void close_socket(socket_t s) noexcept { if (s >= 0) { ::close(s); } }
        bool socket_ok(socket_t s) noexcept { return s >= 0; }
        void net_init() {}
        #endif

        bool send_all(socket_t s, const uint8_t* data, size_t size) {
            size_t sent = 0;
            while (sent < size) {
                #ifdef _WIN32
                const int rc = ::send(s, reinterpret_cast<const char*>(data + sent), static_cast<int>(size - sent), 0);
                #else
                const ssize_t rc = ::send(s, data + sent, size - sent, 0);
                #endif
                if (rc <= 0) { return false; }
                sent += static_cast<size_t>(rc);
            }
            return true;
        }

        bool recv_all(socket_t s, uint8_t* data, size_t size) {
            size_t got = 0;
            while (got < size) {
                #ifdef _WIN32
                const int rc = ::recv(s, reinterpret_cast<char*>(data + got), static_cast<int>(size - got), 0);
                #else
                const ssize_t rc = ::recv(s, data + got, size - got, 0);
                #endif
                if (rc <= 0) { return false; }
                got += static_cast<size_t>(rc);
            }
            return true;
        }

        bool send_all(net::TlsStream& stream, const uint8_t* data, size_t size) {
            size_t sent = 0;
            while (sent < size) { sent += stream.send(data + sent, size - sent); }
            return true;
        }

        bool recv_all(net::TlsStream& stream, uint8_t* data, size_t size) {
            size_t got = 0;
            while (got < size) { got += stream.recv(data + got, size - got); }
            return true;
        }

        uint32_t read_u32(const uint8_t* b) noexcept {
            return static_cast<uint32_t>(b[0]) | (static_cast<uint32_t>(b[1]) << 8) | (static_cast<uint32_t>(b[2]) << 16) | (static_cast<uint32_t>(b[3]) << 24);
        }

        bool recv_frame(socket_t s, DecodedFrame& out) {
            uint8_t header[ReplFrameHeader::SIZE];
            if (!recv_all(s, header, sizeof(header))) { return false; }
            const uint32_t payload_len = read_u32(header + 6);
            std::vector<uint8_t> wire(sizeof(header) + payload_len);
            std::memcpy(wire.data(), header, sizeof(header));
            if (payload_len > 0 && !recv_all(s, wire.data() + sizeof(header), payload_len)) { return false; }
            return decode_frame(wire, out);
        }

        bool recv_frame(net::TlsStream& stream, DecodedFrame& out) {
            uint8_t header[ReplFrameHeader::SIZE];
            try {
                if (!recv_all(stream, header, sizeof(header))) { return false; }
                const uint32_t payload_len = read_u32(header + 6);
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

        socket_t listen_on(uint16_t port) {
            net_init();
            socket_t s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
            if (!socket_ok(s)) { throw std::runtime_error("ReplicationServer: socket failed"); }
            int reuse = 1;
            ::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&reuse), sizeof(reuse));
            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            addr.sin_port = htons(port);
            if (::bind(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
                close_socket(s);
                throw std::runtime_error("ReplicationServer: bind failed");
            }
            if (::listen(s, 16) != 0) {
                close_socket(s);
                throw std::runtime_error("ReplicationServer: listen failed");
            }
            return s;
        }

        struct BufferedWire {
            uint64_t seq = 0;
            std::vector<uint8_t> wire;
        };

        struct ReplicaState {
            socket_t sock = BAD_SOCKET;
            std::unique_ptr<net::TlsStream> tls;
            uint64_t node_id = 0;
            std::atomic<uint64_t> last_acked_seq{0};
            std::atomic<bool> dead{false};
            std::mutex queue_mutex;
            std::condition_variable queue_cv;
            std::deque<std::vector<uint8_t>> queue;
            std::thread send_thread;
            std::thread recv_thread;

            ~ReplicaState() {
                if (tls) { tls->close(); }
                close_socket(sock);
            }
        };
    } // namespace

    class ReplicationServer::Impl {
        public:
            uint16_t repl_port = 0;
            uint64_t self_node_id = 0;
            std::function<uint64_t()> get_current_seq;
            AckPolicy ack_policy;
            ClusterRuntimeOptions runtime_options;

            socket_t listen_sock = BAD_SOCKET;
            std::atomic<bool> running{false};
            std::thread accept_thread;

            mutable std::mutex replicas_mutex;
            std::vector<std::shared_ptr<ReplicaState>> replicas;
            std::deque<BufferedWire> entry_buffer;

            std::mutex ack_mutex;
            std::condition_variable ack_cv;

            void start() {
                listen_sock = listen_on(repl_port);
                running.store(true);
                accept_thread = std::thread([this] { accept_loop(); });
            }

            void close() {
                running.store(false);
                shutdown_socket(listen_sock);
                close_socket(listen_sock);
                listen_sock = BAD_SOCKET;

                std::vector<std::shared_ptr<ReplicaState>> copy;
                {
                    std::lock_guard lock{replicas_mutex};
                    copy = replicas;
                    replicas.clear();
                }
                for (auto& replica : copy) {
                    replica->dead.store(true);
                    close_replica(replica);
                    replica->queue_cv.notify_all();
                }
                if (accept_thread.joinable()) { accept_thread.join(); }
                for (auto& replica : copy) {
                    if (replica->send_thread.joinable()) { replica->send_thread.join(); }
                    if (replica->recv_thread.joinable()) { replica->recv_thread.join(); }
                }
            }

            void accept_loop() {
                while (running.load()) {
                    socket_t client = ::accept(listen_sock, nullptr, nullptr);
                    if (!socket_ok(client)) { break; }

                    std::unique_ptr<net::TlsStream> tls;
                    if (runtime_options.transport_mode == TransportMode::TLS) {
                        try {
                            auto storage = make_tls_config(runtime_options);
                            tls = std::make_unique<net::TlsStream>();
                            tls->accept(static_cast<std::uintptr_t>(client), storage.config);
                            client = BAD_SOCKET;
                        }
                        catch (...) {
                            close_socket(client);
                            continue;
                        }
                    }

                    DecodedFrame frame;
                    ClientHello hello;
                    if (!recv_frame_from(client, tls.get(), frame) || frame.type != ReplMsgType::ClientHello || !decode_client_hello(frame.payload, hello)) {
                        if (tls) { tls->close(); }
                        close_socket(client);
                        continue;
                    }

                    ServerHello response{};
                    response.node_id = self_node_id;
                    response.current_seq = get_current_seq ? get_current_seq() : 0;
                    response.role = NodeRole::Primary;
                    auto hello_wire = encode_server_hello(response);
                    if (!send_to(client, tls.get(), hello_wire.data(), hello_wire.size())) {
                        if (tls) { tls->close(); }
                        close_socket(client);
                        continue;
                    }

                    auto replica = std::make_shared<ReplicaState>();
                    replica->sock = client;
                    replica->tls = std::move(tls);
                    replica->node_id = hello.node_id;
                    replica->last_acked_seq.store(hello.last_seq);

                    {
                        std::lock_guard lock{replicas_mutex};
                        for (const auto& buffered : entry_buffer) {
                            if (buffered.seq == 0 || buffered.seq > hello.last_seq) { replica->queue.push_back(buffered.wire); }
                        }
                        replicas.push_back(replica);
                    }
                    replica->send_thread = std::thread([this, replica] { send_loop(replica); });
                    replica->recv_thread = std::thread([this, replica] { recv_loop(replica); });
                    replica->queue_cv.notify_one();
                }
            }

            static bool send_to(socket_t sock, net::TlsStream* tls, const uint8_t* data, size_t size) {
                try { return tls != nullptr ? send_all(*tls, data, size) : send_all(sock, data, size); }
                catch (...) { return false; }
            }

            static bool recv_frame_from(socket_t sock, net::TlsStream* tls, DecodedFrame& out) {
                return tls != nullptr ? recv_frame(*tls, out) : recv_frame(sock, out);
            }

            static void close_replica(const std::shared_ptr<ReplicaState>& replica) {
                if (replica->tls) { replica->tls->shutdown(); }
                else {
                    shutdown_socket(replica->sock);
                    close_socket(replica->sock);
                }
                replica->sock = BAD_SOCKET;
            }

            void send_loop(const std::shared_ptr<ReplicaState>& replica) {
                while (!replica->dead.load()) {
                    std::vector<uint8_t> wire;
                    {
                        std::unique_lock lock{replica->queue_mutex};
                        replica->queue_cv.wait(lock, [&] { return replica->dead.load() || !replica->queue.empty(); });
                        if (replica->dead.load() && replica->queue.empty()) { break; }
                        wire = std::move(replica->queue.front());
                        replica->queue.pop_front();
                    }
                    if (!send_to(replica->sock, replica->tls.get(), wire.data(), wire.size())) {
                        replica->dead.store(true);
                        close_replica(replica);
                        break;
                    }
                }
            }

            void recv_loop(const std::shared_ptr<ReplicaState>& replica) {
                while (!replica->dead.load()) {
                    DecodedFrame frame;
                    if (!recv_frame_from(replica->sock, replica->tls.get(), frame)) { break; }
                    if (frame.type != ReplMsgType::Ack) { break; }
                    ReplAck ack;
                    if (!decode_ack(frame.payload, ack)) { break; }
                    replica->last_acked_seq.store(ack.seq);
                    ack_cv.notify_all();
                }
                replica->dead.store(true);
                close_replica(replica);
                replica->queue_cv.notify_all();
            }

            void enqueue_wire(uint64_t seq, std::vector<uint8_t> wire) {
                {
                    std::lock_guard lock{replicas_mutex};
                    entry_buffer.push_back(BufferedWire{seq, wire});
                    while (entry_buffer.size() > ENTRY_BUFFER_SIZE) { entry_buffer.pop_front(); }
                    for (auto& replica : replicas) {
                        if (!replica->dead.load()) {
                            std::lock_guard qlock{replica->queue_mutex};
                            replica->queue.push_back(wire);
                            replica->queue_cv.notify_one();
                        }
                    }
                }
                wait_for_acks(seq);
            }

            void wait_for_acks(uint64_t seq) {
                if (ack_policy.mode == AckPolicyMode::Async || seq == 0) { return; }
                const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
                while (std::chrono::steady_clock::now() < deadline) {
                    size_t live = 0;
                    size_t acked = 0;
                    {
                        std::lock_guard lock{replicas_mutex};
                        for (const auto& replica : replicas) {
                            if (!replica->dead.load()) {
                                ++live;
                                if (replica->last_acked_seq.load() >= seq) { ++acked; }
                            }
                        }
                    }

                    const bool ok = ack_policy.mode == AckPolicyMode::All ? acked >= live : acked >= ack_policy.quorum;
                    if (ok) { return; }

                    std::unique_lock lock{ack_mutex};
                    ack_cv.wait_for(lock, std::chrono::milliseconds(50));
                }
            }
    };

    ReplicationServer::ReplicationServer(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {}
    ReplicationServer::~ReplicationServer() { close(); }

    std::unique_ptr<ReplicationServer> ReplicationServer::create(
        uint16_t repl_port,
        uint64_t self_node_id,
        std::function<uint64_t()> get_current_seq,
        AckPolicy ack_policy,
        ClusterRuntimeOptions runtime_options
    ) {
        auto impl = std::make_unique<Impl>();
        impl->repl_port = repl_port;
        impl->self_node_id = self_node_id;
        impl->get_current_seq = std::move(get_current_seq);
        impl->ack_policy = ack_policy;
        impl->runtime_options = std::move(runtime_options);
        return std::unique_ptr < ReplicationServer > (new ReplicationServer(std::move(impl)));
    }

    void ReplicationServer::start() { impl_->start(); }
    void ReplicationServer::close() { if (impl_) { impl_->close(); } }

    void ReplicationServer::ship_entry(
        uint64_t seq,
        ReplOpType op,
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint8_t record_flags,
        uint64_t source_node_id
    ) {
        ReplEntry entry;
        entry.seq = seq;
        entry.source_node_id = source_node_id;
        entry.op = op;
        entry.record_flags = record_flags;
        entry.key.assign(key.begin(), key.end());
        entry.value.assign(value.begin(), value.end());
        impl_->enqueue_wire(seq, encode_entry(entry));
    }

    void ReplicationServer::ship_blob(uint64_t seq, uint64_t blob_id, std::span<const uint8_t> content) {
        ReplBlob blob;
        blob.seq = seq;
        blob.blob_id = blob_id;
        blob.content.assign(content.begin(), content.end());
        impl_->enqueue_wire(0, encode_blob(blob));
    }

    size_t ReplicationServer::replica_count() const noexcept {
        std::lock_guard lock{impl_->replicas_mutex};
        size_t count = 0;
        for (const auto& replica : impl_->replicas) { if (!replica->dead.load()) { ++count; } }
        return count;
    }
} // namespace akkaradb::engine::cluster

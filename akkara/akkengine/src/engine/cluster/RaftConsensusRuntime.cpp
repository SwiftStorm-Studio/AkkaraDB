#include "RaftConsensusRuntime.hpp"
#include "akk/crypto/SecureChannel.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <mutex>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
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
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace akkaradb::engine::cluster {
    namespace {
        using Clock = std::chrono::steady_clock;

#ifdef _WIN32
        using SocketHandle = SOCKET;
        constexpr SocketHandle BAD_SOCKET = INVALID_SOCKET;
        void netInit() {
            static std::once_flag once;
            std::call_once(
                once,
                [] {
                    WSADATA wsa{};
                    if (::WSAStartup(MAKEWORD(2, 2), &wsa) != 0) { throw std::runtime_error("RaftConsensusRuntime: WSAStartup failed"); }
                }
            );
        }
        void closeSocket(SocketHandle s) noexcept { if (s != BAD_SOCKET) { ::closesocket(s); } }
        void shutdownSocket(SocketHandle s) noexcept { if (s != BAD_SOCKET) { ::shutdown(s, SD_BOTH); } }
        bool socketOk(SocketHandle s) noexcept { return s != INVALID_SOCKET; }
#else
        using SocketHandle = int;
        constexpr SocketHandle BAD_SOCKET = -1;
        void netInit() {}
        void closeSocket(SocketHandle s) noexcept { if (s >= 0) { ::close(s); } }
        void shutdownSocket(SocketHandle s) noexcept { if (s >= 0) { ::shutdown(s, SHUT_RDWR); } }
        bool socketOk(SocketHandle s) noexcept { return s >= 0; }
#endif

        void writeU8(std::vector<uint8_t>& out, uint8_t value) { out.push_back(value); }
        void writeU32(std::vector<uint8_t>& out, uint32_t value) {
            for (size_t i = 0; i < 4; ++i) { out.push_back(static_cast<uint8_t>(value >> (8 * i))); }
        }
        void writeU64(std::vector<uint8_t>& out, uint64_t value) {
            for (size_t i = 0; i < 8; ++i) { out.push_back(static_cast<uint8_t>(value >> (8 * i))); }
        }
        uint32_t readU32(std::span<const uint8_t> in, size_t off) {
            return static_cast<uint32_t>(in[off]) | (static_cast<uint32_t>(in[off + 1]) << 8) |
                (static_cast<uint32_t>(in[off + 2]) << 16) | (static_cast<uint32_t>(in[off + 3]) << 24);
        }
        uint64_t readU64(std::span<const uint8_t> in, size_t off) {
            uint64_t out = 0;
            for (size_t i = 0; i < 8; ++i) { out |= static_cast<uint64_t>(in[off + i]) << (8 * i); }
            return out;
        }
        bool readBytes(std::span<const uint8_t> in, size_t& cursor, size_t len, std::vector<uint8_t>& out) {
            if (cursor + len > in.size()) { return false; }
            out.assign(in.begin() + static_cast<std::ptrdiff_t>(cursor), in.begin() + static_cast<std::ptrdiff_t>(cursor + len));
            cursor += len;
            return true;
        }

        bool sendAll(SocketHandle s, const uint8_t* data, size_t size) {
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

        bool recvAll(SocketHandle s, uint8_t* data, size_t size) {
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

        void setTimeouts(SocketHandle s, int timeoutMs) {
#ifdef _WIN32
            const DWORD timeout = static_cast<DWORD>(timeoutMs);
            ::setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&timeout), sizeof(timeout));
            ::setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&timeout), sizeof(timeout));
#else
            timeval tv{};
            tv.tv_sec = timeoutMs / 1000;
            tv.tv_usec = (timeoutMs % 1000) * 1000;
            ::setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            ::setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
#endif
        }

        SocketHandle listenOn(const std::string& host, uint16_t port) {
            netInit();
            addrinfo hints{};
            hints.ai_family = AF_UNSPEC;
            hints.ai_socktype = SOCK_STREAM;
            hints.ai_flags = AI_PASSIVE;

            addrinfo* result = nullptr;
            const auto portText = std::to_string(port);
            if (::getaddrinfo(host.empty() ? nullptr : host.c_str(), portText.c_str(), &hints, &result) != 0) {
                throw std::runtime_error("RaftConsensusRuntime: getaddrinfo failed for listener");
            }

            SocketHandle out = BAD_SOCKET;
            for (addrinfo* it = result; it != nullptr; it = it->ai_next) {
                SocketHandle s = ::socket(it->ai_family, it->ai_socktype, it->ai_protocol);
                if (!socketOk(s)) { continue; }
                int reuse = 1;
                ::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&reuse), sizeof(reuse));
                if (::bind(s, it->ai_addr, static_cast<int>(it->ai_addrlen)) == 0 && ::listen(s, 32) == 0) {
                    out = s;
                    break;
                }
                closeSocket(s);
            }
            ::freeaddrinfo(result);
            if (!socketOk(out)) { throw std::runtime_error("RaftConsensusRuntime: bind/listen failed"); }
            return out;
        }

        SocketHandle connectTo(const std::string& host, uint16_t port, int timeoutMs) {
            netInit();
            addrinfo hints{};
            hints.ai_family = AF_UNSPEC;
            hints.ai_socktype = SOCK_STREAM;
            addrinfo* result = nullptr;
            const auto portText = std::to_string(port);
            if (::getaddrinfo(host.c_str(), portText.c_str(), &hints, &result) != 0) { return BAD_SOCKET; }
            SocketHandle out = BAD_SOCKET;
            for (addrinfo* it = result; it != nullptr; it = it->ai_next) {
                SocketHandle s = ::socket(it->ai_family, it->ai_socktype, it->ai_protocol);
                if (!socketOk(s)) { continue; }
                setTimeouts(s, timeoutMs);
                if (::connect(s, it->ai_addr, static_cast<int>(it->ai_addrlen)) == 0) {
                    out = s;
                    break;
                }
                closeSocket(s);
            }
            ::freeaddrinfo(result);
            return out;
        }

        bool recvFrame(SocketHandle s, DecodedFrame& out) {
            uint8_t header[ReplFrameHeader::SIZE];
            if (!recvAll(s, header, sizeof(header))) { return false; }
            const uint32_t payloadLen = static_cast<uint32_t>(header[6]) | (static_cast<uint32_t>(header[7]) << 8) |
                (static_cast<uint32_t>(header[8]) << 16) | (static_cast<uint32_t>(header[9]) << 24);
            std::vector<uint8_t> wire(sizeof(header) + payloadLen);
            std::memcpy(wire.data(), header, sizeof(header));
            if (payloadLen > 0 && !recvAll(s, wire.data() + sizeof(header), payloadLen)) { return false; }
            return decodeFrame(wire, out);
        }

        bool sendFrame(SocketHandle s, const std::vector<uint8_t>& wire) { return sendAll(s, wire.data(), wire.size()); }

        constexpr uint32_t SECURE_HELLO_MAGIC = 0x48434B41;
        constexpr uint32_t SECURE_FRAME_MAGIC = 0x46434B41;
        constexpr uint8_t SECURE_VERSION = 1;
        constexpr uint8_t SECURE_CLIENT_HELLO = 1;
        constexpr uint8_t SECURE_SERVER_HELLO = 2;
        constexpr size_t SECURE_HELLO_HEADER_SIZE = 6;
        constexpr size_t SECURE_CLIENT_HELLO_SIZE = SECURE_HELLO_HEADER_SIZE + 64;
        constexpr size_t SECURE_SERVER_HELLO_SIZE = SECURE_HELLO_HEADER_SIZE + 80;
        constexpr size_t SECURE_FRAME_HEADER_SIZE = 34;
        constexpr uint32_t SECURE_MAX_CIPHERTEXT_SIZE = 128u * 1024u * 1024u;

        void writeU32Le(uint8_t* out, uint32_t value) noexcept {
            for (size_t i = 0; i < 4; ++i) { out[i] = static_cast<uint8_t>(value >> (i * 8)); }
        }

        void writeU64Le(uint8_t* out, uint64_t value) noexcept {
            for (size_t i = 0; i < 8; ++i) { out[i] = static_cast<uint8_t>(value >> (i * 8)); }
        }

        uint32_t readU32Le(const uint8_t* in) noexcept {
            return static_cast<uint32_t>(in[0]) | (static_cast<uint32_t>(in[1]) << 8) | (static_cast<uint32_t>(in[2]) << 16) |
                (static_cast<uint32_t>(in[3]) << 24);
        }

        uint64_t readU64Le(const uint8_t* in) noexcept {
            uint64_t out = 0;
            for (size_t i = 0; i < 8; ++i) { out |= static_cast<uint64_t>(in[i]) << (i * 8); }
            return out;
        }

        crypto::NodeIdentity loadSecureIdentity(const ClusterRuntimeOptions& options) {
            if (options.secure.identitySeedPath.empty()) { return crypto::generateNodeIdentity(); }
            return crypto::IdentityStore{options.secure.identitySeedPath}.loadOrCreate();
        }

        std::optional<crypto::PublicKey> pinnedPeerKey(const ClusterRuntimeOptions& options, uint64_t nodeId) {
            if (nodeId == 0) { return std::nullopt; }
            for (const auto& pin : options.secure.pinnedPeers) { if (pin.nodeId == nodeId) { return pin.publicKey; } }
            return std::nullopt;
        }

        bool writeSecureClientHello(SocketHandle socket, const crypto::ClientHello& hello) {
            std::array<uint8_t, SECURE_CLIENT_HELLO_SIZE> wire{};
            writeU32Le(wire.data(), SECURE_HELLO_MAGIC);
            wire[4] = SECURE_VERSION;
            wire[5] = SECURE_CLIENT_HELLO;
            std::memcpy(wire.data() + SECURE_HELLO_HEADER_SIZE, hello.staticPublicKey.data(), hello.staticPublicKey.size());
            std::memcpy(
                wire.data() + SECURE_HELLO_HEADER_SIZE + hello.staticPublicKey.size(),
                hello.ephemeralPublicKey.data(),
                hello.ephemeralPublicKey.size()
            );
            return sendAll(socket, wire.data(), wire.size());
        }

        bool readSecureClientHello(SocketHandle socket, crypto::ClientHello& hello) {
            std::array<uint8_t, SECURE_CLIENT_HELLO_SIZE> wire{};
            if (!recvAll(socket, wire.data(), wire.size())) { return false; }
            if (readU32Le(wire.data()) != SECURE_HELLO_MAGIC || wire[4] != SECURE_VERSION || wire[5] != SECURE_CLIENT_HELLO) {
                return false;
            }
            std::memcpy(hello.staticPublicKey.data(), wire.data() + SECURE_HELLO_HEADER_SIZE, hello.staticPublicKey.size());
            std::memcpy(
                hello.ephemeralPublicKey.data(),
                wire.data() + SECURE_HELLO_HEADER_SIZE + hello.staticPublicKey.size(),
                hello.ephemeralPublicKey.size()
            );
            return true;
        }

        bool writeSecureServerHello(SocketHandle socket, const crypto::ServerHello& hello) {
            std::array<uint8_t, SECURE_SERVER_HELLO_SIZE> wire{};
            writeU32Le(wire.data(), SECURE_HELLO_MAGIC);
            wire[4] = SECURE_VERSION;
            wire[5] = SECURE_SERVER_HELLO;
            std::memcpy(wire.data() + SECURE_HELLO_HEADER_SIZE, hello.staticPublicKey.data(), hello.staticPublicKey.size());
            std::memcpy(
                wire.data() + SECURE_HELLO_HEADER_SIZE + hello.staticPublicKey.size(),
                hello.ephemeralPublicKey.data(),
                hello.ephemeralPublicKey.size()
            );
            std::memcpy(
                wire.data() + SECURE_HELLO_HEADER_SIZE + hello.staticPublicKey.size() + hello.ephemeralPublicKey.size(),
                hello.authenticator.data(),
                hello.authenticator.size()
            );
            return sendAll(socket, wire.data(), wire.size());
        }

        bool readSecureServerHello(SocketHandle socket, crypto::ServerHello& hello) {
            std::array<uint8_t, SECURE_SERVER_HELLO_SIZE> wire{};
            if (!recvAll(socket, wire.data(), wire.size())) { return false; }
            if (readU32Le(wire.data()) != SECURE_HELLO_MAGIC || wire[4] != SECURE_VERSION || wire[5] != SECURE_SERVER_HELLO) {
                return false;
            }
            std::memcpy(hello.staticPublicKey.data(), wire.data() + SECURE_HELLO_HEADER_SIZE, hello.staticPublicKey.size());
            std::memcpy(
                hello.ephemeralPublicKey.data(),
                wire.data() + SECURE_HELLO_HEADER_SIZE + hello.staticPublicKey.size(),
                hello.ephemeralPublicKey.size()
            );
            std::memcpy(
                hello.authenticator.data(),
                wire.data() + SECURE_HELLO_HEADER_SIZE + hello.staticPublicKey.size() + hello.ephemeralPublicKey.size(),
                hello.authenticator.size()
            );
            return true;
        }

        bool sendSecureFrame(SocketHandle socket, crypto::SecureSession& session, const uint8_t* data, size_t size) {
            if (size > SECURE_MAX_CIPHERTEXT_SIZE) { return false; }
            const auto encrypted = session.seal(std::span<const uint8_t>{data, size});
            if (encrypted.ciphertext.size() > SECURE_MAX_CIPHERTEXT_SIZE) { return false; }

            std::array<uint8_t, SECURE_FRAME_HEADER_SIZE> header{};
            writeU32Le(header.data(), SECURE_FRAME_MAGIC);
            header[4] = SECURE_VERSION;
            header[5] = 0;
            writeU64Le(header.data() + 6, encrypted.counter);
            writeU32Le(header.data() + 14, static_cast<uint32_t>(encrypted.ciphertext.size()));
            std::memcpy(header.data() + 18, encrypted.tag.data(), encrypted.tag.size());
            return sendAll(socket, header.data(), header.size()) &&
                (encrypted.ciphertext.empty() || sendAll(socket, encrypted.ciphertext.data(), encrypted.ciphertext.size()));
        }

        bool recvSecureFrame(SocketHandle socket, crypto::SecureSession& session, DecodedFrame& out) {
            std::array<uint8_t, SECURE_FRAME_HEADER_SIZE> header{};
            if (!recvAll(socket, header.data(), header.size())) { return false; }
            if (readU32Le(header.data()) != SECURE_FRAME_MAGIC || header[4] != SECURE_VERSION) { return false; }

            crypto::EncryptedFrame encrypted;
            encrypted.counter = readU64Le(header.data() + 6);
            const uint32_t ciphertextSize = readU32Le(header.data() + 14);
            if (ciphertextSize > SECURE_MAX_CIPHERTEXT_SIZE) { return false; }
            std::memcpy(encrypted.tag.data(), header.data() + 18, encrypted.tag.size());
            encrypted.ciphertext.resize(ciphertextSize);
            if (ciphertextSize > 0 && !recvAll(socket, encrypted.ciphertext.data(), ciphertextSize)) { return false; }

            std::vector<uint8_t> plaintext;
            if (!session.open(encrypted, plaintext)) { return false; }
            return decodeFrame(plaintext, out);
        }

        bool sendFrame(SocketHandle s, crypto::SecureSession* secure, const std::vector<uint8_t>& wire) {
            if (secure != nullptr) { return sendSecureFrame(s, *secure, wire.data(), wire.size()); }
            return sendFrame(s, wire);
        }

        bool recvFrame(SocketHandle s, crypto::SecureSession* secure, DecodedFrame& out) {
            if (secure != nullptr) { return recvSecureFrame(s, *secure, out); }
            return recvFrame(s, out);
        }

        enum class RaftRole : uint8_t {
            FOLLOWER, CANDIDATE, LEADER,
        };

        struct RaftLogEntry {
            uint64_t term = 0;
            uint64_t index = 0;
            ReplOpType op = ReplOpType::PUT;
            uint8_t flags = 0;
            uint64_t sourceNodeId = 0;
            std::vector<uint8_t> key;
            std::vector<uint8_t> value;
        };

        struct RequestVote {
            uint64_t term = 0;
            uint64_t candidateId = 0;
            uint64_t lastLogIndex = 0;
            uint64_t lastLogTerm = 0;
        };

        struct RequestVoteResponse {
            uint64_t term = 0;
            bool voteGranted = false;
        };

        struct AppendEntries {
            uint64_t term = 0;
            uint64_t leaderId = 0;
            uint64_t prevLogIndex = 0;
            uint64_t prevLogTerm = 0;
            uint64_t leaderCommit = 0;
            std::vector<RaftLogEntry> entries;
        };

        struct AppendEntriesResponse {
            uint64_t term = 0;
            bool success = false;
            uint64_t matchIndex = 0;
        };

        std::vector<uint8_t> encodeEntryPayload(const RaftLogEntry& entry) {
            std::vector<uint8_t> out;
            writeU64(out, entry.term);
            writeU64(out, entry.index);
            writeU8(out, static_cast<uint8_t>(entry.op));
            writeU8(out, entry.flags);
            writeU64(out, entry.sourceNodeId);
            writeU32(out, static_cast<uint32_t>(entry.key.size()));
            writeU32(out, static_cast<uint32_t>(entry.value.size()));
            out.insert(out.end(), entry.key.begin(), entry.key.end());
            out.insert(out.end(), entry.value.begin(), entry.value.end());
            return out;
        }

        bool decodeEntryPayload(std::span<const uint8_t> in, size_t& cursor, RaftLogEntry& out) {
            if (cursor + 34 > in.size()) { return false; }
            out.term = readU64(in, cursor);
            cursor += 8;
            out.index = readU64(in, cursor);
            cursor += 8;
            out.op = static_cast<ReplOpType>(in[cursor++]);
            out.flags = in[cursor++];
            out.sourceNodeId = readU64(in, cursor);
            cursor += 8;
            const uint32_t keyLen = readU32(in, cursor);
            cursor += 4;
            const uint32_t valueLen = readU32(in, cursor);
            cursor += 4;
            return readBytes(in, cursor, keyLen, out.key) && readBytes(in, cursor, valueLen, out.value);
        }

        std::vector<uint8_t> encodeRequestVote(const RequestVote& rpc) {
            std::vector<uint8_t> out;
            writeU64(out, rpc.term);
            writeU64(out, rpc.candidateId);
            writeU64(out, rpc.lastLogIndex);
            writeU64(out, rpc.lastLogTerm);
            return encodeFrame(ReplMsgType::RAFT_REQUEST_VOTE, out);
        }

        bool decodeRequestVote(std::span<const uint8_t> in, RequestVote& out) {
            if (in.size() != 32) { return false; }
            out.term = readU64(in, 0);
            out.candidateId = readU64(in, 8);
            out.lastLogIndex = readU64(in, 16);
            out.lastLogTerm = readU64(in, 24);
            return true;
        }

        std::vector<uint8_t> encodeRequestVoteResponse(const RequestVoteResponse& rpc) {
            std::vector<uint8_t> out;
            writeU64(out, rpc.term);
            writeU8(out, rpc.voteGranted ? 1 : 0);
            return encodeFrame(ReplMsgType::RAFT_REQUEST_VOTE_RESPONSE, out);
        }

        bool decodeRequestVoteResponse(std::span<const uint8_t> in, RequestVoteResponse& out) {
            if (in.size() != 9) { return false; }
            out.term = readU64(in, 0);
            out.voteGranted = in[8] != 0;
            return true;
        }

        std::vector<uint8_t> encodeAppendEntries(const AppendEntries& rpc) {
            std::vector<uint8_t> out;
            writeU64(out, rpc.term);
            writeU64(out, rpc.leaderId);
            writeU64(out, rpc.prevLogIndex);
            writeU64(out, rpc.prevLogTerm);
            writeU64(out, rpc.leaderCommit);
            writeU32(out, static_cast<uint32_t>(rpc.entries.size()));
            for (const auto& entry : rpc.entries) {
                const auto payload = encodeEntryPayload(entry);
                writeU32(out, static_cast<uint32_t>(payload.size()));
                out.insert(out.end(), payload.begin(), payload.end());
            }
            return encodeFrame(ReplMsgType::RAFT_APPEND_ENTRIES, out);
        }

        bool decodeAppendEntries(std::span<const uint8_t> in, AppendEntries& out) {
            if (in.size() < 44) { return false; }
            out.term = readU64(in, 0);
            out.leaderId = readU64(in, 8);
            out.prevLogIndex = readU64(in, 16);
            out.prevLogTerm = readU64(in, 24);
            out.leaderCommit = readU64(in, 32);
            const uint32_t count = readU32(in, 40);
            size_t cursor = 44;
            out.entries.clear();
            out.entries.reserve(count);
            for (uint32_t i = 0; i < count; ++i) {
                if (cursor + 4 > in.size()) { return false; }
                const uint32_t entryLen = readU32(in, cursor);
                cursor += 4;
                if (cursor + entryLen > in.size()) { return false; }
                size_t entryCursor = 0;
                RaftLogEntry entry;
                if (!decodeEntryPayload(in.subspan(cursor, entryLen), entryCursor, entry) || entryCursor != entryLen) { return false; }
                cursor += entryLen;
                out.entries.push_back(std::move(entry));
            }
            return cursor == in.size();
        }

        std::vector<uint8_t> encodeAppendEntriesResponse(const AppendEntriesResponse& rpc) {
            std::vector<uint8_t> out;
            writeU64(out, rpc.term);
            writeU8(out, rpc.success ? 1 : 0);
            writeU64(out, rpc.matchIndex);
            return encodeFrame(ReplMsgType::RAFT_APPEND_ENTRIES_RESPONSE, out);
        }

        bool decodeAppendEntriesResponse(std::span<const uint8_t> in, AppendEntriesResponse& out) {
            if (in.size() != 17) { return false; }
            out.term = readU64(in, 0);
            out.success = in[8] != 0;
            out.matchIndex = readU64(in, 9);
            return true;
        }

        bool sameEntry(const RaftLogEntry& lhs, const RaftLogEntry& rhs) {
            return lhs.term == rhs.term && lhs.index == rhs.index && lhs.op == rhs.op && lhs.flags == rhs.flags &&
                lhs.sourceNodeId == rhs.sourceNodeId && lhs.key == rhs.key && lhs.value == rhs.value;
        }
    }

    class RaftConsensusRuntime::Impl {
        public:
            Impl(
                std::filesystem::path dbDir,
                ClusterConfig config,
                uint64_t selfNodeId,
                ClusterEngineCallbacks callbacks,
                ClusterRuntimeOptions runtimeOptions
            )
                : dbDir_{std::move(dbDir)},
                  statePath_{dbDir_.empty() ? std::filesystem::path{"cluster-raft.state"} : dbDir_ / "cluster-raft.state"},
                  logPath_{dbDir_.empty() ? std::filesystem::path{"cluster-raft.log"} : dbDir_ / "cluster-raft.log"},
                  config_{std::move(config)},
                  router_{config_},
                  selfNodeId_{selfNodeId},
                  callbacks_{std::move(callbacks)},
                  runtimeOptions_{std::move(runtimeOptions)} {
                config_.validate();
                self_ = config_.findById(selfNodeId_);
                if (self_ == nullptr || !self_->dataBearing()) {
                    throw std::invalid_argument("RaftConsensusRuntime: local node must be data-bearing");
                }
                if (runtimeOptions_.transportMode == TransportMode::SECURE) { localIdentity_ = loadSecureIdentity(runtimeOptions_); }
                for (const auto& node : config_.nodes()) {
                    if (node.dataBearing() && node.nodeId != selfNodeId_) { peers_.push_back(node); }
                }
                if (config_.dataNodes().empty()) { throw std::invalid_argument("RaftConsensusRuntime: no data-bearing nodes"); }
                recoverState();
                recoverLog();
                if (commitIndex_ == 0 && callbacks_.getLastSeq) { commitIndex_ = std::min(lastLogIndex(), callbacks_.getLastSeq()); }
            }

            ~Impl() { close(); }

            void start() {
                {
                    std::lock_guard lock{mutex_};
                    if (running_) { return; }
                    running_ = true;
                    electionDeadline_ = nextElectionDeadline();
                }
                listenSock_ = listenOn(runtimeOptions_.replBindHost, self_->replPort);
                acceptThread_ = std::thread([this] { acceptLoop(); });
                timerThread_ = std::thread([this] { timerLoop(); });
                applyCommitted();
                if (peers_.empty()) { becomeLeader(currentTerm_ == 0 ? 1 : currentTerm_); }
            }

            void close() {
                {
                    std::lock_guard lock{mutex_};
                    running_ = false;
                }
                shutdownSocket(listenSock_);
                closeSocket(listenSock_);
                listenSock_ = BAD_SOCKET;
                cv_.notify_all();
                if (acceptThread_.joinable()) { acceptThread_.join(); }
                if (timerThread_.joinable()) { timerThread_.join(); }
                std::vector<std::thread> clients;
                {
                    std::lock_guard lock{clientThreadsMutex_};
                    clients.swap(clientThreads_);
                }
                for (auto& client : clients) {
                    if (client.joinable()) { client.join(); }
                }
            }

            NodeRole role() const noexcept {
                const auto role = role_.load();
                if (role == RaftRole::LEADER) { return NodeRole::PRIMARY; }
                return NodeRole::REPLICA;
            }

            const ClusterRouter& router() const noexcept { return router_; }

            void shipEntry(
                uint64_t seq,
                ReplOpType op,
                std::span<const uint8_t> key,
                std::span<const uint8_t> value,
                uint8_t recordFlags,
                uint64_t sourceNodeId
            ) {
                RaftLogEntry entry;
                uint64_t term = 0;
                {
                    std::lock_guard lock{mutex_};
                    if (role_.load() != RaftRole::LEADER) { throw std::runtime_error("RaftConsensusRuntime: local node is not Raft leader"); }
                    if (commitIndex_ != lastLogIndex()) {
                        throw std::runtime_error("RaftConsensusRuntime: cannot accept a new proposal while a prior entry is uncommitted");
                    }
                    if (seq != lastLogIndex() + 1) {
                        throw std::runtime_error("RaftConsensusRuntime: proposed sequence is not the next Raft log index");
                    }
                    term = currentTerm_;
                    entry.term = term;
                    entry.index = seq;
                    entry.op = op;
                    entry.flags = recordFlags;
                    entry.sourceNodeId = sourceNodeId;
                    entry.key.assign(key.begin(), key.end());
                    entry.value.assign(value.begin(), value.end());
                    log_.push_back(entry);
                    persistLog();
                }

                if (!replicateEntryToMajority(entry)) {
                    throw std::runtime_error("RaftConsensusRuntime: failed to replicate entry to Raft majority");
                }

                {
                    std::lock_guard lock{mutex_};
                    if (role_.load() != RaftRole::LEADER || currentTerm_ != term) {
                        throw std::runtime_error("RaftConsensusRuntime: leadership changed before commit");
                    }
                    if (entry.index > commitIndex_) {
                        commitIndex_ = entry.index;
                        persistLog();
                    }
                }
                sendHeartbeats();
            }

            void shipBlob(uint64_t, uint64_t, std::span<const uint8_t>) {
                if (role_.load() != RaftRole::LEADER) { throw std::runtime_error("RaftConsensusRuntime: local node is not Raft leader"); }
            }

        private:
            Clock::time_point nextElectionDeadline() {
                static thread_local std::mt19937_64 rng{std::random_device{}()};
                std::uniform_int_distribution<int> dist(350, 700);
                return Clock::now() + std::chrono::milliseconds(dist(rng));
            }

            size_t quorum() const {
                const size_t voters = peers_.size() + 1;
                return (voters / 2) + 1;
            }

            uint64_t lastLogIndex() const noexcept { return log_.empty() ? 0 : log_.back().index; }
            uint64_t lastLogTerm() const noexcept { return log_.empty() ? 0 : log_.back().term; }

            std::optional<RaftLogEntry> entryAt(uint64_t index) const {
                if (index == 0) { return std::nullopt; }
                for (const auto& entry : log_) { if (entry.index == index) { return entry; } }
                return std::nullopt;
            }

            uint64_t termAt(uint64_t index) const {
                if (index == 0) { return 0; }
                const auto entry = entryAt(index);
                return entry ? entry->term : 0;
            }

            void persistState() {
                if (statePath_.has_parent_path()) { std::filesystem::create_directories(statePath_.parent_path()); }
                const auto tmp = statePath_.parent_path() / (statePath_.filename().string() + ".tmp");
                std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
                if (!out) { throw std::runtime_error("RaftConsensusRuntime: cannot write state"); }
                out.write("AKRS1", 5);
                out.write(reinterpret_cast<const char*>(&currentTerm_), sizeof(currentTerm_));
                out.write(reinterpret_cast<const char*>(&votedFor_), sizeof(votedFor_));
                out.close();
                std::filesystem::rename(tmp, statePath_);
            }

            void recoverState() {
                std::ifstream in(statePath_, std::ios::binary);
                if (!in) { return; }
                char magic[5]{};
                in.read(magic, sizeof(magic));
                if (std::string_view{magic, sizeof(magic)} != "AKRS1") { throw std::runtime_error("RaftConsensusRuntime: bad state file"); }
                in.read(reinterpret_cast<char*>(&currentTerm_), sizeof(currentTerm_));
                in.read(reinterpret_cast<char*>(&votedFor_), sizeof(votedFor_));
                if (!in) { throw std::runtime_error("RaftConsensusRuntime: truncated state file"); }
            }

            void persistLog() {
                if (logPath_.has_parent_path()) { std::filesystem::create_directories(logPath_.parent_path()); }
                const auto tmp = logPath_.parent_path() / (logPath_.filename().string() + ".tmp");
                std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
                if (!out) { throw std::runtime_error("RaftConsensusRuntime: cannot write log"); }
                out.write("AKRL1", 5);
                out.write(reinterpret_cast<const char*>(&commitIndex_), sizeof(commitIndex_));
                const uint64_t count = static_cast<uint64_t>(log_.size());
                out.write(reinterpret_cast<const char*>(&count), sizeof(count));
                for (const auto& entry : log_) {
                    const auto payload = encodeEntryPayload(entry);
                    const uint64_t len = static_cast<uint64_t>(payload.size());
                    out.write(reinterpret_cast<const char*>(&len), sizeof(len));
                    out.write(reinterpret_cast<const char*>(payload.data()), static_cast<std::streamsize>(payload.size()));
                }
                out.close();
                std::filesystem::rename(tmp, logPath_);
            }

            void recoverLog() {
                std::ifstream in(logPath_, std::ios::binary);
                if (!in) { return; }
                char magic[5]{};
                in.read(magic, sizeof(magic));
                if (std::string_view{magic, sizeof(magic)} != "AKRL1") { throw std::runtime_error("RaftConsensusRuntime: bad log file"); }
                in.read(reinterpret_cast<char*>(&commitIndex_), sizeof(commitIndex_));
                uint64_t count = 0;
                in.read(reinterpret_cast<char*>(&count), sizeof(count));
                for (uint64_t i = 0; i < count; ++i) {
                    uint64_t len = 0;
                    in.read(reinterpret_cast<char*>(&len), sizeof(len));
                    std::vector<uint8_t> payload(static_cast<size_t>(len));
                    in.read(reinterpret_cast<char*>(payload.data()), static_cast<std::streamsize>(payload.size()));
                    size_t cursor = 0;
                    RaftLogEntry entry;
                    if (!decodeEntryPayload(payload, cursor, entry) || cursor != payload.size()) {
                        throw std::runtime_error("RaftConsensusRuntime: corrupt log entry");
                    }
                    log_.push_back(std::move(entry));
                }
                if (!in) { throw std::runtime_error("RaftConsensusRuntime: truncated log file"); }
            }

            void setRole(RaftRole role) {
                const auto old = role_.exchange(role);
                if (old == role) { return; }
                if (callbacks_.roleChange) { callbacks_.roleChange(role == RaftRole::LEADER ? NodeRole::PRIMARY : NodeRole::REPLICA); }
            }

            void becomeFollower(uint64_t term) {
                bool changed = false;
                {
                    std::lock_guard lock{mutex_};
                    if (term > currentTerm_) {
                        currentTerm_ = term;
                        votedFor_ = 0;
                        persistState();
                        changed = true;
                    }
                    electionDeadline_ = nextElectionDeadline();
                }
                setRole(RaftRole::FOLLOWER);
                (void)changed;
            }

            void becomeLeader(uint64_t term) {
                {
                    std::lock_guard lock{mutex_};
                    currentTerm_ = std::max(currentTerm_, term);
                    votedFor_ = selfNodeId_;
                    persistState();
                    electionDeadline_ = Clock::now() + std::chrono::hours(24);
                }
                setRole(RaftRole::LEADER);
                sendHeartbeats();
            }

            void timerLoop() {
                while (true) {
                    {
                        std::unique_lock lock{mutex_};
                        if (!running_) { return; }
                        if (role_.load() == RaftRole::LEADER) {
                            lock.unlock();
                            sendHeartbeats();
                            std::this_thread::sleep_for(std::chrono::milliseconds(100));
                            continue;
                        }
                        if (Clock::now() < electionDeadline_) {
                            cv_.wait_until(lock, electionDeadline_);
                            continue;
                        }
                    }
                    startElection();
                }
            }

            void startElection() {
                uint64_t term = 0;
                uint64_t lastIndex = 0;
                uint64_t lastTerm = 0;
                {
                    std::lock_guard lock{mutex_};
                    if (!running_) { return; }
                    if (!self_->coordinatorEligible()) {
                        electionDeadline_ = nextElectionDeadline();
                        return;
                    }
                    role_.store(RaftRole::CANDIDATE);
                    ++currentTerm_;
                    term = currentTerm_;
                    votedFor_ = selfNodeId_;
                    lastIndex = lastLogIndex();
                    lastTerm = lastLogTerm();
                    electionDeadline_ = nextElectionDeadline();
                    persistState();
                }

                std::atomic<size_t> votes{1};
                std::vector<std::thread> workers;
                for (const auto& peer : peers_) {
                    workers.emplace_back(
                        [this, peer, term, lastIndex, lastTerm, &votes] {
                            RequestVoteResponse response;
                            if (requestVote(peer, RequestVote{term, selfNodeId_, lastIndex, lastTerm}, response)) {
                                if (response.term > term) {
                                    becomeFollower(response.term);
                                    return;
                                }
                                if (response.voteGranted) { votes.fetch_add(1); }
                            }
                        }
                    );
                }
                for (auto& worker : workers) {
                    if (worker.joinable()) { worker.join(); }
                }

                bool won = false;
                {
                    std::lock_guard lock{mutex_};
                    won = running_ && currentTerm_ == term && role_.load() == RaftRole::CANDIDATE && votes.load() >= quorum();
                }
                if (won) { becomeLeader(term); }
            }

            bool requestVote(const NodeInfo& peer, const RequestVote& request, RequestVoteResponse& response) {
                SocketHandle socket = connectTo(peer.host, peer.replPort, 500);
                if (!socketOk(socket)) { return false; }
                const auto close = [&] { closeSocket(socket); };
                std::unique_ptr<crypto::SecureSession> secure;
                if (runtimeOptions_.transportMode == TransportMode::SECURE) {
                    secure = openSecureSession(socket, peer.nodeId);
                    if (!secure) {
                        close();
                        return false;
                    }
                }
                if (!sendFrame(socket, secure.get(), encodeRequestVote(request))) {
                    close();
                    return false;
                }
                DecodedFrame frame;
                if (!recvFrame(socket, secure.get(), frame) || frame.type != ReplMsgType::RAFT_REQUEST_VOTE_RESPONSE) {
                    close();
                    return false;
                }
                close();
                return decodeRequestVoteResponse(frame.payload, response);
            }

            bool appendEntries(const NodeInfo& peer, const AppendEntries& request, AppendEntriesResponse& response) {
                SocketHandle socket = connectTo(peer.host, peer.replPort, 1000);
                if (!socketOk(socket)) { return false; }
                const auto close = [&] { closeSocket(socket); };
                std::unique_ptr<crypto::SecureSession> secure;
                if (runtimeOptions_.transportMode == TransportMode::SECURE) {
                    secure = openSecureSession(socket, peer.nodeId);
                    if (!secure) {
                        close();
                        return false;
                    }
                }
                if (!sendFrame(socket, secure.get(), encodeAppendEntries(request))) {
                    close();
                    return false;
                }
                DecodedFrame frame;
                if (!recvFrame(socket, secure.get(), frame) || frame.type != ReplMsgType::RAFT_APPEND_ENTRIES_RESPONSE) {
                    close();
                    return false;
                }
                close();
                return decodeAppendEntriesResponse(frame.payload, response);
            }

            std::unique_ptr<crypto::SecureSession> openSecureSession(SocketHandle socket, uint64_t peerNodeId) {
                try {
                    crypto::NoiseInitiator initiator{localIdentity_};
                    if (!writeSecureClientHello(socket, initiator.hello())) { return nullptr; }
                    crypto::ServerHello serverHello{};
                    if (!readSecureServerHello(socket, serverHello)) { return nullptr; }
                    auto session = initiator.finish(serverHello, pinnedPeerKey(runtimeOptions_, peerNodeId));
                    return std::make_unique<crypto::SecureSession>(std::move(session));
                }
                catch (...) {
                    return nullptr;
                }
            }

            std::unique_ptr<crypto::SecureSession> acceptSecureSession(SocketHandle socket, crypto::PublicKey& remotePublicKey) {
                try {
                    crypto::ClientHello hello{};
                    if (!readSecureClientHello(socket, hello)) { return nullptr; }
                    auto accepted = crypto::acceptResponder(localIdentity_, hello);
                    remotePublicKey = accepted.remoteStaticPublicKey;
                    if (!writeSecureServerHello(socket, accepted.hello)) { return nullptr; }
                    return std::make_unique<crypto::SecureSession>(std::move(accepted.session));
                }
                catch (...) {
                    return nullptr;
                }
            }

            bool verifySecurePeer(uint64_t nodeId, const crypto::PublicKey& remotePublicKey) const {
                if (runtimeOptions_.transportMode != TransportMode::SECURE) { return true; }
                const auto expected = pinnedPeerKey(runtimeOptions_, nodeId);
                return !expected || remotePublicKey == *expected;
            }

            bool replicateEntryToMajority(const RaftLogEntry& entry) {
                if (quorum() == 1) { return true; }
                std::atomic<size_t> replicated{1};
                std::vector<std::thread> workers;
                for (const auto& peer : peers_) {
                    workers.emplace_back(
                        [this, peer, entry, &replicated] {
                            AppendEntries request;
                            {
                                std::lock_guard lock{mutex_};
                                request.term = currentTerm_;
                                request.leaderId = selfNodeId_;
                                request.prevLogIndex = entry.index - 1;
                                request.prevLogTerm = termAt(request.prevLogIndex);
                                request.leaderCommit = commitIndex_;
                                request.entries.push_back(entry);
                            }
                            AppendEntriesResponse response;
                            if (appendEntries(peer, request, response)) {
                                if (response.term > request.term) {
                                    becomeFollower(response.term);
                                    return;
                                }
                                if (response.success && response.matchIndex >= entry.index) {
                                    replicated.fetch_add(1);
                                    return;
                                }
                            }

                            AppendEntries catchup;
                            {
                                std::lock_guard lock{mutex_};
                                catchup.term = currentTerm_;
                                catchup.leaderId = selfNodeId_;
                                catchup.prevLogIndex = 0;
                                catchup.prevLogTerm = 0;
                                catchup.leaderCommit = commitIndex_;
                                catchup.entries = log_;
                            }
                            if (appendEntries(peer, catchup, response)) {
                                if (response.term > catchup.term) {
                                    becomeFollower(response.term);
                                    return;
                                }
                                if (response.success && response.matchIndex >= entry.index) { replicated.fetch_add(1); }
                            }
                        }
                    );
                }
                for (auto& worker : workers) {
                    if (worker.joinable()) { worker.join(); }
                }
                return replicated.load() >= quorum();
            }

            void sendHeartbeats() {
                uint64_t term = 0;
                uint64_t commit = 0;
                uint64_t lastIndex = 0;
                uint64_t lastTerm = 0;
                {
                    std::lock_guard lock{mutex_};
                    if (!running_ || role_.load() != RaftRole::LEADER) { return; }
                    term = currentTerm_;
                    commit = commitIndex_;
                    lastIndex = lastLogIndex();
                    lastTerm = lastLogTerm();
                }
                for (const auto& peer : peers_) {
                    AppendEntries request{
                        .term = term,
                        .leaderId = selfNodeId_,
                        .prevLogIndex = lastIndex,
                        .prevLogTerm = lastTerm,
                        .leaderCommit = commit,
                        .entries = {},
                    };
                    AppendEntriesResponse response;
                    if (appendEntries(peer, request, response) && response.term > term) { becomeFollower(response.term); }
                }
            }

            void acceptLoop() {
                while (true) {
                    {
                        std::lock_guard lock{mutex_};
                        if (!running_) { return; }
                    }
                    SocketHandle client = ::accept(listenSock_, nullptr, nullptr);
                    if (!socketOk(client)) { return; }
                    setTimeouts(client, 1000);
                    std::lock_guard lock{clientThreadsMutex_};
                    clientThreads_.emplace_back([this, client] { handleClient(client); });
                }
            }

            void handleClient(SocketHandle client) {
                std::unique_ptr<crypto::SecureSession> secure;
                crypto::PublicKey remotePublicKey{};
                if (runtimeOptions_.transportMode == TransportMode::SECURE) {
                    secure = acceptSecureSession(client, remotePublicKey);
                    if (!secure) {
                        closeSocket(client);
                        return;
                    }
                }
                DecodedFrame frame;
                if (!recvFrame(client, secure.get(), frame)) {
                    closeSocket(client);
                    return;
                }
                if (frame.type == ReplMsgType::RAFT_REQUEST_VOTE) {
                    RequestVote request;
                    RequestVoteResponse response;
                    if (decodeRequestVote(frame.payload, request) && verifySecurePeer(request.candidateId, remotePublicKey)) {
                        response = handleRequestVote(request);
                    }
                    (void)sendFrame(client, secure.get(), encodeRequestVoteResponse(response));
                }
                else if (frame.type == ReplMsgType::RAFT_APPEND_ENTRIES) {
                    AppendEntries request;
                    AppendEntriesResponse response;
                    if (decodeAppendEntries(frame.payload, request) && verifySecurePeer(request.leaderId, remotePublicKey)) {
                        response = handleAppendEntries(request);
                    }
                    (void)sendFrame(client, secure.get(), encodeAppendEntriesResponse(response));
                }
                closeSocket(client);
            }

            RequestVoteResponse handleRequestVote(const RequestVote& request) {
                std::lock_guard lock{mutex_};
                if (request.term < currentTerm_) { return RequestVoteResponse{.term = currentTerm_, .voteGranted = false}; }
                if (request.term > currentTerm_) {
                    currentTerm_ = request.term;
                    votedFor_ = 0;
                    setRole(RaftRole::FOLLOWER);
                }
                const bool upToDate = request.lastLogTerm > lastLogTerm() ||
                    (request.lastLogTerm == lastLogTerm() && request.lastLogIndex >= lastLogIndex());
                const bool canVote = votedFor_ == 0 || votedFor_ == request.candidateId;
                const bool granted = canVote && upToDate;
                if (granted) {
                    votedFor_ = request.candidateId;
                    electionDeadline_ = nextElectionDeadline();
                }
                persistState();
                return RequestVoteResponse{.term = currentTerm_, .voteGranted = granted};
            }

            AppendEntriesResponse handleAppendEntries(const AppendEntries& request) {
                AppendEntriesResponse response;
                {
                    std::lock_guard lock{mutex_};
                    if (request.term < currentTerm_) {
                        return AppendEntriesResponse{.term = currentTerm_, .success = false, .matchIndex = lastLogIndex()};
                    }
                    if (request.term > currentTerm_) {
                        currentTerm_ = request.term;
                        votedFor_ = 0;
                        persistState();
                    }
                    setRole(RaftRole::FOLLOWER);
                    electionDeadline_ = nextElectionDeadline();

                    if (request.prevLogIndex > lastLogIndex() || termAt(request.prevLogIndex) != request.prevLogTerm) {
                        return AppendEntriesResponse{.term = currentTerm_, .success = false, .matchIndex = lastLogIndex()};
                    }

                    for (const auto& entry : request.entries) {
                        const auto existing = entryAt(entry.index);
                        if (existing && sameEntry(*existing, entry)) { continue; }
                        log_.erase(
                            std::remove_if(log_.begin(), log_.end(), [&](const RaftLogEntry& item) { return item.index >= entry.index; }),
                            log_.end()
                        );
                        log_.push_back(entry);
                    }

                    if (request.leaderCommit > commitIndex_) { commitIndex_ = std::min(request.leaderCommit, lastLogIndex()); }
                    persistLog();
                    response = AppendEntriesResponse{.term = currentTerm_, .success = true, .matchIndex = lastLogIndex()};
                }
                applyCommitted();
                return response;
            }

            void applyCommitted() {
                std::lock_guard applyLock{applyMutex_};
                std::vector<RaftLogEntry> toApply;
                {
                    std::lock_guard lock{mutex_};
                    const uint64_t engineLast = callbacks_.getLastSeq ? callbacks_.getLastSeq() : lastApplied_;
                    if (engineLast > lastApplied_) { lastApplied_ = engineLast; }
                    for (const auto& entry : log_) {
                        if (entry.index > lastApplied_ && entry.index <= commitIndex_) { toApply.push_back(entry); }
                    }
                }
                for (const auto& entry : toApply) {
                    if (callbacks_.apply) {
                        callbacks_.apply(entry.index, entry.op, entry.key, entry.value, entry.flags, entry.sourceNodeId);
                    }
                    if (callbacks_.forceDurable) { callbacks_.forceDurable(); }
                    std::lock_guard lock{mutex_};
                    if (entry.index > lastApplied_) { lastApplied_ = entry.index; }
                }
            }

            std::filesystem::path dbDir_;
            std::filesystem::path statePath_;
            std::filesystem::path logPath_;
            ClusterConfig config_;
            ClusterRouter router_;
            uint64_t selfNodeId_ = 0;
            const NodeInfo* self_ = nullptr;
            std::vector<NodeInfo> peers_;
            ClusterEngineCallbacks callbacks_;
            ClusterRuntimeOptions runtimeOptions_;
            crypto::NodeIdentity localIdentity_{};

            mutable std::mutex mutex_;
            std::condition_variable cv_;
            bool running_ = false;
            uint64_t currentTerm_ = 0;
            uint64_t votedFor_ = 0;
            uint64_t commitIndex_ = 0;
            uint64_t lastApplied_ = 0;
            std::vector<RaftLogEntry> log_;
            Clock::time_point electionDeadline_{};
            std::atomic<RaftRole> role_{RaftRole::FOLLOWER};
            std::mutex applyMutex_;

            SocketHandle listenSock_ = BAD_SOCKET;
            std::thread acceptThread_;
            std::thread timerThread_;
            std::mutex clientThreadsMutex_;
            std::vector<std::thread> clientThreads_;
    };

    std::unique_ptr<RaftConsensusRuntime> RaftConsensusRuntime::create(
        std::filesystem::path dbDir,
        ClusterConfig config,
        uint64_t selfNodeId,
        ClusterEngineCallbacks callbacks,
        ClusterRuntimeOptions runtimeOptions
    ) {
        return std::unique_ptr<RaftConsensusRuntime>(
            new RaftConsensusRuntime(std::make_unique<Impl>(std::move(dbDir), std::move(config), selfNodeId, std::move(callbacks), std::move(runtimeOptions)))
        );
    }

    RaftConsensusRuntime::RaftConsensusRuntime(std::unique_ptr<Impl> impl) : impl_{std::move(impl)} {}
    RaftConsensusRuntime::~RaftConsensusRuntime() = default;
    void RaftConsensusRuntime::start() { impl_->start(); }
    void RaftConsensusRuntime::close() { impl_->close(); }
    NodeRole RaftConsensusRuntime::role() const noexcept { return impl_->role(); }
    const ClusterRouter& RaftConsensusRuntime::router() const noexcept { return impl_->router(); }
    void RaftConsensusRuntime::shipEntry(
        uint64_t seq,
        ReplOpType op,
        std::span<const uint8_t> key,
        std::span<const uint8_t> value,
        uint8_t recordFlags,
        uint64_t sourceNodeId
    ) { impl_->shipEntry(seq, op, key, value, recordFlags, sourceNodeId); }
    void RaftConsensusRuntime::shipBlob(uint64_t seq, uint64_t blobId, std::span<const uint8_t> content) {
        impl_->shipBlob(seq, blobId, content);
    }
}

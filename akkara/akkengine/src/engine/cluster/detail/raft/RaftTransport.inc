/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */


// Included inside the anonymous namespace in RaftConsensusRuntime.cpp.
void configureNoSigPipe(SocketHandle s) noexcept {
    #if !defined(_WIN32) && defined(SO_NOSIGPIPE)
    int enabled = 1; (void)::setsockopt(s, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
    #else
    (void)s;
    #endif
}

bool sendAll(SocketHandle s, const uint8_t* data, size_t size) {
    size_t sent = 0;
    while (sent < size) {
        #ifdef _WIN32
        const auto rc = ::send(s, reinterpret_cast<const char*>(data + sent), static_cast<int>(size - sent), 0);
        if (rc < 0 && ::WSAGetLastError() == WSAEINTR) { continue; }
        #else
        int flags = 0;
        #ifdef MSG_NOSIGNAL
        flags |= MSG_NOSIGNAL;
        #endif
        const ssize_t rc = ::send(s, data + sent, size - sent, flags); if (rc < 0 && errno == EINTR) { continue; }
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
        const auto rc = ::recv(s, reinterpret_cast<char*>(data + got), static_cast<int>(size - got), 0);
        if (rc < 0 && ::WSAGetLastError() == WSAEINTR) { continue; }
        #else
        const ssize_t rc = ::recv(s, data + got, size - got, 0); if (rc < 0 && errno == EINTR) { continue; }
        #endif
        if (rc <= 0) { return false; }
        got += static_cast<size_t>(rc);
    }
    return true;
}

void setTimeouts(SocketHandle s, int timeoutMs) {
    #ifdef _WIN32
    const auto timeout = static_cast<DWORD>(timeoutMs);
    ::setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&timeout), sizeof(timeout));
    ::setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&timeout), sizeof(timeout));
    #else
    timeval tv{}; tv.tv_sec = timeoutMs / 1000; tv.tv_usec = (timeoutMs % 1000) * 1000; ::setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
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
        configureNoSigPipe(s);
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

bool recvFrame(
    SocketHandle s,
    DecodedFrame& out,
    const std::shared_ptr<RaftReceiveBudget>& budget,
    uint32_t maxPayload = ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE
) {
    uint8_t header[ReplFrameHeader::SIZE];
    if (!recvAll(s, header, sizeof(header))) { return false; }
    const auto payloadLen = static_cast<uint32_t>(header[6]) | (static_cast<uint32_t>(header[7]) << 8) | (static_cast<uint32_t>(header[8]) << 16) |
        (static_cast<uint32_t>(header[9]) << 24);
    if (payloadLen > maxPayload) { return false; }
    auto reservation = budget->reserve(ReplFrameHeader::SIZE + static_cast<uint64_t>(payloadLen) * 2);
    std::vector<uint8_t> wire(sizeof(header) + payloadLen);
    std::memcpy(wire.data(), header, sizeof(header));
    if (payloadLen > 0 && !recvAll(s, wire.data() + sizeof(header), payloadLen)) { return false; }
    if (!decodeFrame(wire, out)) { return false; }
    out.memoryReservation = std::move(reservation);
    return true;
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
constexpr uint32_t SECURE_MAX_CIPHERTEXT_SIZE = ReplFrameHeader::SIZE + ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE;

void writeU32Le(uint8_t* out, uint32_t value) noexcept { for (size_t i = 0; i < 4; ++i) { out[i] = static_cast<uint8_t>(value >> (i * 8)); } }

void writeU64Le(uint8_t* out, uint64_t value) noexcept { for (size_t i = 0; i < 8; ++i) { out[i] = static_cast<uint8_t>(value >> (i * 8)); } }

uint32_t readU32Le(const uint8_t* in) noexcept {
    return static_cast<uint32_t>(in[0]) | (static_cast<uint32_t>(in[1]) << 8) | (static_cast<uint32_t>(in[2]) << 16) | (static_cast<uint32_t>(in[3]) <<
        24);
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
    const auto pin = std::ranges::find(options.secure.pinnedPeers, nodeId, &ClusterPeerPublicKeyPin::nodeId);
    if (pin == options.secure.pinnedPeers.end()) { return std::nullopt; }
    return pin->publicKey;
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
    if (readU32Le(wire.data()) != SECURE_HELLO_MAGIC || wire[4] != SECURE_VERSION || wire[5] != SECURE_CLIENT_HELLO) { return false; }
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
    if (readU32Le(wire.data()) != SECURE_HELLO_MAGIC || wire[4] != SECURE_VERSION || wire[5] != SECURE_SERVER_HELLO) { return false; }
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
    return sendAll(socket, header.data(), header.size()) && (encrypted.ciphertext.empty() || sendAll(
        socket,
        encrypted.ciphertext.data(),
        encrypted.ciphertext.size()
    ));
}

bool recvSecureFrame(
    SocketHandle socket,
    crypto::SecureSession& session,
    DecodedFrame& out,
    const std::shared_ptr<RaftReceiveBudget>& budget,
    uint32_t maxPayload = ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE
) {
    std::array<uint8_t, SECURE_FRAME_HEADER_SIZE> header{};
    if (!recvAll(socket, header.data(), header.size())) { return false; }
    if (readU32Le(header.data()) != SECURE_FRAME_MAGIC || header[4] != SECURE_VERSION) { return false; }

    crypto::EncryptedFrame encrypted;
    encrypted.counter = readU64Le(header.data() + 6);
    const auto ciphertextSize = readU32Le(header.data() + 14);
    if (ciphertextSize > ReplFrameHeader::SIZE + maxPayload) { return false; }
    auto reservation = budget->reserve(static_cast<uint64_t>(ciphertextSize) * 3);
    std::memcpy(encrypted.tag.data(), header.data() + 18, encrypted.tag.size());
    encrypted.ciphertext.resize(ciphertextSize);
    if (ciphertextSize > 0 && !recvAll(socket, encrypted.ciphertext.data(), ciphertextSize)) { return false; }

    std::vector<uint8_t> plaintext;
    if (!session.open(encrypted, plaintext)) { return false; }
    if (!decodeFrame(plaintext, out)) { return false; }
    out.memoryReservation = std::move(reservation);
    return true;
}

bool sendFrame(SocketHandle s, crypto::SecureSession* secure, const std::vector<uint8_t>& wire) {
    if (secure != nullptr) { return sendSecureFrame(s, *secure, wire.data(), wire.size()); }
    return sendFrame(s, wire);
}

bool recvFrame(
    SocketHandle s,
    crypto::SecureSession* secure,
    DecodedFrame& out,
    const std::shared_ptr<RaftReceiveBudget>& budget,
    uint32_t maxPayload = ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE
) {
    if (secure != nullptr) { return recvSecureFrame(s, *secure, out, budget, maxPayload); }
    return recvFrame(s, out, budget, maxPayload);
}


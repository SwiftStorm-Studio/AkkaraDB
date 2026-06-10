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

// akkengine/src/net/tls/TlsStream.cpp
#include "akk/net/tls/TlsStream.hpp"

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <cerrno>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#endif

#include <mbedtls/ctr_drbg.h>
#include <mbedtls/entropy.h>
#include <mbedtls/net_sockets.h>
#include <mbedtls/pk.h>
#include <mbedtls/ssl.h>
#include <mbedtls/x509_crt.h>

#include <array>
#include <cstring>
#include <cstdio>
#include <mutex>
#include <stdexcept>
#include <string>

namespace akkaradb::net {
    namespace {
        #ifdef _WIN32
        using NativeSocket = SOCKET;
        constexpr NativeSocket INVALID_NATIVE_SOCKET = INVALID_SOCKET;
        #else
        using NativeSocket = int; constexpr NativeSocket INVALID_NATIVE_SOCKET = -1;
        #endif

        constexpr std::array<unsigned char, 32> DEFAULT_CLUSTER_PSK{
            0x41,
            0x6b,
            0x6b,
            0x61,
            0x72,
            0x61,
            0x44,
            0x42,
            0x53,
            0x50,
            0x45,
            0x43,
            0x76,
            0x35,
            0x54,
            0x4c,
            0x53,
            0x50,
            0x53,
            0x4b,
            0x21,
            0x63,
            0x6c,
            0x75,
            0x73,
            0x74,
            0x65,
            0x72,
            0x21,
            0x30,
            0x31,
            0x00,
        };
        constexpr const char* DEFAULT_CLUSTER_IDENTITY = "akkaradb-cluster";

        [[nodiscard]] std::string portToString(uint16_t port) {
            char buf[6]{};
            std::snprintf(buf, sizeof(buf), "%u", static_cast<unsigned>(port));
            return buf;
        }

        [[noreturn]] void throwMbedtls(const char* what, int code) {
            throw std::runtime_error(std::string{what} + " failed: " + std::to_string(code));
        }

        [[nodiscard]] bool socketValid(NativeSocket socket) noexcept {
            #ifdef _WIN32
            return socket != INVALID_SOCKET;
            #else
            return socket >= 0;
            #endif
        }

        void closeNativeSocket(NativeSocket& socket) noexcept {
            if (!socketValid(socket)) { return; }
            #ifdef _WIN32
            ::closesocket(socket);
            #else
            ::close(socket);
            #endif
            socket = INVALID_NATIVE_SOCKET;
        }

        void shutdownNativeSocket(NativeSocket socket) noexcept {
            if (!socketValid(socket)) { return; }
            #ifdef _WIN32
            ::shutdown(socket, SD_BOTH);
            #else
            ::shutdown(socket, SHUT_RDWR);
            #endif
        }

        void setNativeSocketTimeout(NativeSocket socket, int option, uint32_t timeoutMs) noexcept {
            if (!socketValid(socket) || timeoutMs == 0) { return; }

            #ifdef _WIN32
            const DWORD value = timeoutMs;
            (void)::setsockopt(socket, SOL_SOCKET, option, reinterpret_cast<const char*>(&value), sizeof(value));
            #else
            timeval value{}; value.tv_sec = static_cast<time_t>(timeoutMs / 1000u); value.tv_usec = static_cast<suseconds_t>((timeoutMs %
                1000u) * 1000u); (void)::setsockopt(socket, SOL_SOCKET, option, &value, static_cast<socklen_t>(sizeof(value)));
            #endif
        }

        void applyNativeSocketTimeouts(NativeSocket socket, uint32_t readTimeoutMs, uint32_t writeTimeoutMs) noexcept {
            setNativeSocketTimeout(socket, SO_RCVTIMEO, readTimeoutMs);
            setNativeSocketTimeout(socket, SO_SNDTIMEO, writeTimeoutMs);
        }

        [[nodiscard]] int sendNoSigpipeFlags() noexcept {
            #ifdef _WIN32
            return 0;
            #else
            #ifdef MSG_NOSIGNAL
            return MSG_NOSIGNAL;
            #else
            return 0;
            #endif
            #endif
        }

        void ensureTlsSocketRuntime() {
            #ifdef _WIN32
            static std::once_flag once;
            std::call_once(
                once,
                [] {
                    WSADATA data{};
                    const int ret = WSAStartup(MAKEWORD(2, 2), &data);
                    if (ret != 0) { throw std::runtime_error("TlsStream: WSAStartup failed: " + std::to_string(ret)); }
                }
            );
            #endif
        }

        [[nodiscard]] NativeSocket connectNativeSocket(const char* host, uint16_t port) {
            ensureTlsSocketRuntime();

            addrinfo hints{};
            hints.ai_family = AF_UNSPEC;
            hints.ai_socktype = SOCK_STREAM;
            #ifdef _WIN32
            hints.ai_protocol = IPPROTO_TCP;
            #else
            hints.ai_protocol = IPPROTO_TCP;
            #endif

            addrinfo* result = nullptr;
            const auto portS = portToString(port);
            const int gai = ::getaddrinfo(host, portS.c_str(), &hints, &result);
            if (gai != 0) { throw std::runtime_error("TlsStream::connect: getaddrinfo failed"); }

            NativeSocket connected = INVALID_NATIVE_SOCKET;
            for (addrinfo* it = result; it != nullptr; it = it->ai_next) {
                const auto candidate = ::socket(it->ai_family, it->ai_socktype, it->ai_protocol);
                if (!socketValid(candidate)) { continue; }

                const int rc = ::connect(
                    candidate,
                    it->ai_addr,
                    #ifdef _WIN32
                    static_cast<int>(it->ai_addrlen)
                    #else
                    it->ai_addrlen
                    #endif
                );
                if (rc == 0) {
                    connected = candidate;
                    break;
                }

                NativeSocket closeCandidate = candidate;
                closeNativeSocket(closeCandidate);
            }

            ::freeaddrinfo(result);
            if (!socketValid(connected)) { throw std::runtime_error("TlsStream::connect: connect failed"); }
            return connected;
        }

        struct SocketBio {
            NativeSocket socket = INVALID_NATIVE_SOCKET;
        };

        int bioSend(void* ctx, const unsigned char* buf, size_t len) noexcept {
            auto* bio = static_cast<SocketBio*>(ctx);
            if (bio == nullptr || !socketValid(bio->socket)) { return MBEDTLS_ERR_NET_INVALID_CONTEXT; }
            #ifdef _WIN32
            const int n = ::send(bio->socket, reinterpret_cast<const char*>(buf), static_cast<int>(len), 0);
            if (n < 0) {
                const int err = WSAGetLastError();
                if (err == WSAEINTR) { return MBEDTLS_ERR_SSL_WANT_WRITE; }
                if (err == WSAEWOULDBLOCK || err == WSAETIMEDOUT) { return MBEDTLS_ERR_NET_SEND_FAILED; }
                if (err == WSAECONNRESET || err == WSAECONNABORTED || err == WSAENOTCONN) { return MBEDTLS_ERR_NET_CONN_RESET; }
                return MBEDTLS_ERR_NET_SEND_FAILED;
            }
            #else
            const ssize_t n = ::send(bio->socket, buf, len, sendNoSigpipeFlags()); if (n < 0) {
                if (errno == EINTR) { return MBEDTLS_ERR_SSL_WANT_WRITE; }
                if (errno == EAGAIN || errno == EWOULDBLOCK) { return MBEDTLS_ERR_NET_SEND_FAILED; }
                if (errno == ECONNRESET || errno == EPIPE || errno == ENOTCONN) { return MBEDTLS_ERR_NET_CONN_RESET; }
                return MBEDTLS_ERR_NET_SEND_FAILED;
            }
            #endif
            return n;
        }

        int bioRecv(void* ctx, unsigned char* buf, size_t len) noexcept {
            auto* bio = static_cast<SocketBio*>(ctx);
            if (bio == nullptr || !socketValid(bio->socket)) { return MBEDTLS_ERR_NET_INVALID_CONTEXT; }
            #ifdef _WIN32
            const int n = ::recv(bio->socket, reinterpret_cast<char*>(buf), static_cast<int>(len), 0);
            if (n < 0) {
                const int err = WSAGetLastError();
                if (err == WSAEINTR) { return MBEDTLS_ERR_SSL_WANT_READ; }
                if (err == WSAEWOULDBLOCK || err == WSAETIMEDOUT) { return MBEDTLS_ERR_NET_RECV_FAILED; }
                if (err == WSAECONNRESET || err == WSAECONNABORTED || err == WSAENOTCONN) { return MBEDTLS_ERR_NET_CONN_RESET; }
                return MBEDTLS_ERR_NET_RECV_FAILED;
            }
            #else
            const ssize_t n = ::recv(bio->socket, buf, len, 0); if (n < 0) {
                if (errno == EINTR) { return MBEDTLS_ERR_SSL_WANT_READ; }
                if (errno == EAGAIN || errno == EWOULDBLOCK) { return MBEDTLS_ERR_NET_RECV_FAILED; }
                if (errno == ECONNRESET || errno == ENOTCONN) { return MBEDTLS_ERR_NET_CONN_RESET; }
                return MBEDTLS_ERR_NET_RECV_FAILED;
            }
            #endif
            if (n == 0) { return MBEDTLS_ERR_NET_CONN_RESET; }
            return n;
        }
    } // namespace

    struct TlsStream::Impl {
        SocketBio bio{};
        mbedtls_ssl_context ssl{};
        mbedtls_ssl_config cfg{};
        mbedtls_entropy_context entropy{};
        mbedtls_ctr_drbg_context drbg{};
        mbedtls_x509_crt ownCert{};
        mbedtls_x509_crt caCert{};
        mbedtls_pk_context ownKey{};

        Impl() {
            mbedtls_ssl_init(&ssl);
            mbedtls_ssl_config_init(&cfg);
            mbedtls_entropy_init(&entropy);
            mbedtls_ctr_drbg_init(&drbg);
            mbedtls_x509_crt_init(&ownCert);
            mbedtls_x509_crt_init(&caCert);
            mbedtls_pk_init(&ownKey);
        }

        ~Impl() {
            mbedtls_ssl_free(&ssl);
            mbedtls_ssl_config_free(&cfg);
            mbedtls_ctr_drbg_free(&drbg);
            mbedtls_entropy_free(&entropy);
            mbedtls_x509_crt_free(&ownCert);
            mbedtls_x509_crt_free(&caCert);
            mbedtls_pk_free(&ownKey);
            closeNativeSocket(bio.socket);
        }
    };

    TlsStream::~TlsStream() { close(); }

    TlsStream::TlsStream(TlsStream&& other) noexcept : impl_(other.impl_) { other.impl_ = nullptr; }

    TlsStream& TlsStream::operator=(TlsStream&& other) noexcept {
        if (this != &other) {
            close();
            impl_ = other.impl_;
            other.impl_ = nullptr;
        }
        return *this;
    }

    void TlsStream::connect(const char* host, uint16_t port, const TlsConfig& config) { connect(host, port, config, 0, 0); }

    void TlsStream::connect(const char* host, uint16_t port, const TlsConfig& config, uint32_t readTimeoutMs, uint32_t writeTimeoutMs) {
        ensureTlsSocketRuntime();
        close();
        if (host == nullptr) { throw std::invalid_argument("TlsStream::connect: host is null"); }

        impl_ = new Impl();
        impl_->bio.socket = connectNativeSocket(host, port);
        applyNativeSocketTimeouts(impl_->bio.socket, readTimeoutMs, writeTimeoutMs);

        try { setup(config, MBEDTLS_SSL_IS_CLIENT, host); }
        catch (...) {
            close();
            throw;
        }
    }

    void TlsStream::accept(std::uintptr_t nativeSocket, const TlsConfig& config) {
        ensureTlsSocketRuntime();
        close();
        impl_ = new Impl();
        impl_->bio.socket = static_cast<NativeSocket>(nativeSocket);

        try { setup(config, MBEDTLS_SSL_IS_SERVER, nullptr); }
        catch (...) {
            close();
            throw;
        }
    }

    void TlsStream::setup(const TlsConfig& config, int endpoint, const char* hostname) {
        int ret = mbedtls_ctr_drbg_seed(&impl_->drbg, mbedtls_entropy_func, &impl_->entropy, nullptr, 0);
        if (ret != 0) { throwMbedtls("mbedtls_ctr_drbg_seed", ret); }

        ret = mbedtls_ssl_config_defaults(&impl_->cfg, endpoint, MBEDTLS_SSL_TRANSPORT_STREAM, MBEDTLS_SSL_PRESET_DEFAULT);
        if (ret != 0) { throwMbedtls("mbedtls_ssl_config_defaults", ret); }

        mbedtls_ssl_conf_rng(&impl_->cfg, mbedtls_ctr_drbg_random, &impl_->drbg);

        const bool hasCert = config.certPath != nullptr && config.certPath[0] != '\0' && config.keyPath != nullptr && config.keyPath[0] !=
            '\0';
        const bool hasCa = config.caPath != nullptr && config.caPath[0] != '\0';

        if (hasCa) {
            ret = mbedtls_x509_crt_parse_file(&impl_->caCert, config.caPath);
            if (ret != 0) { throwMbedtls("mbedtls_x509_crt_parse_file(ca)", ret); }
            mbedtls_ssl_conf_ca_chain(&impl_->cfg, &impl_->caCert, nullptr);
        }

        if (hasCert) {
            ret = mbedtls_x509_crt_parse_file(&impl_->ownCert, config.certPath);
            if (ret != 0) { throwMbedtls("mbedtls_x509_crt_parse_file(cert)", ret); }
            ret = mbedtls_pk_parse_keyfile(&impl_->ownKey, config.keyPath, nullptr, mbedtls_ctr_drbg_random, &impl_->drbg);
            if (ret != 0) { throwMbedtls("mbedtls_pk_parse_keyfile", ret); }
            ret = mbedtls_ssl_conf_own_cert(&impl_->cfg, &impl_->ownCert, &impl_->ownKey);
            if (ret != 0) { throwMbedtls("mbedtls_ssl_conf_own_cert", ret); }
        }

        if (!hasCert) {
            const unsigned char* psk = config.psk != nullptr ? config.psk : DEFAULT_CLUSTER_PSK.data();
            const size_t pskLen = config.psk != nullptr ? config.pskLen : DEFAULT_CLUSTER_PSK.size();
            const char* identity = config.pskIdentity != nullptr ? config.pskIdentity : DEFAULT_CLUSTER_IDENTITY;
            ret = mbedtls_ssl_conf_psk(&impl_->cfg, psk, pskLen, reinterpret_cast<const unsigned char*>(identity), std::strlen(identity));
            if (ret != 0) { throwMbedtls("mbedtls_ssl_conf_psk", ret); }
            mbedtls_ssl_conf_authmode(&impl_->cfg, MBEDTLS_SSL_VERIFY_NONE);
        }
        else { mbedtls_ssl_conf_authmode(&impl_->cfg, config.verifyPeer && hasCa ? MBEDTLS_SSL_VERIFY_REQUIRED : MBEDTLS_SSL_VERIFY_NONE); }

        ret = mbedtls_ssl_setup(&impl_->ssl, &impl_->cfg);
        if (ret != 0) { throwMbedtls("mbedtls_ssl_setup", ret); }

        if (hostname != nullptr && hasCa) {
            ret = mbedtls_ssl_set_hostname(&impl_->ssl, hostname);
            if (ret != 0) { throwMbedtls("mbedtls_ssl_set_hostname", ret); }
        }

        mbedtls_ssl_set_bio(&impl_->ssl, &impl_->bio, bioSend, bioRecv, nullptr);

        for (;;) {
            ret = mbedtls_ssl_handshake(&impl_->ssl);
            if (ret == 0) { break; }
            if (ret == MBEDTLS_ERR_SSL_WANT_READ || ret == MBEDTLS_ERR_SSL_WANT_WRITE) { continue; }
            throwMbedtls("mbedtls_ssl_handshake", ret);
        }

        if (config.verifyPeer && hasCa) {
            const uint32_t flags = mbedtls_ssl_get_verify_result(&impl_->ssl);
            if (flags != 0) { throw std::runtime_error("TlsStream: peer certificate verification failed"); }
        }
    }

    std::size_t TlsStream::send(const void* data, std::size_t size) {
        if (impl_ == nullptr) { throw std::runtime_error("tls send on closed stream"); }
        for (;;) {
            const int ret = mbedtls_ssl_write(&impl_->ssl, static_cast<const unsigned char*>(data), size);
            if (ret > 0) { return static_cast<std::size_t>(ret); }
            if (ret == MBEDTLS_ERR_SSL_WANT_READ || ret == MBEDTLS_ERR_SSL_WANT_WRITE) { continue; }
            throwMbedtls("mbedtls_ssl_write", ret);
        }
    }

    std::size_t TlsStream::recv(void* data, std::size_t size) {
        if (impl_ == nullptr) { throw std::runtime_error("tls recv on closed stream"); }
        for (;;) {
            const int ret = mbedtls_ssl_read(&impl_->ssl, static_cast<unsigned char*>(data), size);
            if (ret > 0) { return static_cast<std::size_t>(ret); }
            if (ret == 0) { throw std::runtime_error("tls connection closed"); }
            if (ret == MBEDTLS_ERR_SSL_WANT_READ || ret == MBEDTLS_ERR_SSL_WANT_WRITE) { continue; }
            throwMbedtls("mbedtls_ssl_read", ret);
        }
    }

    void TlsStream::close() noexcept {
        if (impl_ != nullptr) {
            if (socketValid(impl_->bio.socket)) { (void)mbedtls_ssl_close_notify(&impl_->ssl); }
            delete impl_;
            impl_ = nullptr;
        }
    }

    void TlsStream::shutdown() noexcept {
        if (impl_ != nullptr) {
            shutdownNativeSocket(impl_->bio.socket);
            closeNativeSocket(impl_->bio.socket);
        }
    }

    bool TlsStream::valid() const noexcept { return impl_ != nullptr && socketValid(impl_->bio.socket); }
} // namespace akkaradb::net

/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
#include "akk/engine/cluster/detail/CancellableSocketReader.hpp"
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <limits>
#include <stdexcept>
#ifdef _WIN32
#include <winsock2.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <sys/socket.h>
#endif
namespace akkaradb::engine::cluster::detail {
    class CancellableSocketReader::Impl {
    public:
        Impl() {
            #ifdef _WIN32
            stopped_ = ::CreateEventW(nullptr, TRUE, FALSE, nullptr);
            ready_ = ::CreateEventW(nullptr, TRUE, FALSE, nullptr);
            if (!stopped_ || !ready_) {
                if (stopped_) { ::CloseHandle(stopped_); }
                if (ready_) { ::CloseHandle(ready_); }
                throw std::runtime_error("Cluster reader: cannot create receive cancellation events");
            }
            #else
            if (::pipe(pipe_) != 0) { throw std::runtime_error("Cluster reader: cannot create receive cancellation pipe"); }
            for (const int fd : pipe_) {
                const auto flags = ::fcntl(fd, F_GETFL, 0);
                if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0 || ::fcntl(fd, F_SETFD, FD_CLOEXEC) != 0) {
                    ::close(pipe_[0]); ::close(pipe_[1]);
                    throw std::runtime_error("Cluster reader: cannot configure receive cancellation pipe");
                }
            }
            #endif
        }
        ~Impl() {
            #ifdef _WIN32
            ::CloseHandle(stopped_); ::CloseHandle(ready_);
            #else
            ::close(pipe_[0]); ::close(pipe_[1]);
            #endif
        }
        void cancel() {
            if (cancelled_.exchange(true)) { return; }
            #ifdef _WIN32
            ::SetEvent(stopped_);
            #else
            const char stop = 1;
            while (::write(pipe_[1], &stop, 1) < 0 && errno == EINTR) { }
            #endif
        }
        void reset() {
            #ifdef _WIN32
            ::ResetEvent(stopped_); ::ResetEvent(ready_);
            #else
            char buffer[16];
            for (;;) {
                const auto count = ::read(pipe_[0], buffer, sizeof(buffer));
                if (count > 0 || (count < 0 && errno == EINTR)) { continue; }
                break;
            }
            #endif
            cancelled_ = false;
        }
        bool cancelled() const { return cancelled_.load(); }
        #ifdef _WIN32
        HANDLE stopped() const { return stopped_; }
        HANDLE ready() const { return ready_; }
        #else
        int stopped() const { return pipe_[0]; }
        #endif
        bool readAll(ConnectionSocket s, uint8_t* data, size_t size, ConnectionDeadline deadline) {
            size_t received = 0;
            while (received < size) {
                if (expired(deadline)) { return false; }
                // A separate stop event/pipe interrupts reads without closing a
                // descriptor still owned by the worker or polling idle peers.
                const auto waitMs = deadline == std::chrono::steady_clock::time_point::max() ? -1ll :
                    std::clamp<int64_t>(std::chrono::ceil<std::chrono::milliseconds>(
                        deadline - std::chrono::steady_clock::now()).count(), 0, std::numeric_limits<int>::max());
                #ifdef _WIN32
                ::ResetEvent(ready());
                WSAOVERLAPPED operation{};
                operation.hEvent = ready();
                WSABUF buffer{};
                buffer.buf = reinterpret_cast<char*>(data + received);
                buffer.len = static_cast<ULONG>(size - received);
                DWORD flags = 0;
                const auto rc = ::WSARecv(s, &buffer, 1, nullptr, &flags, &operation, nullptr);
                if (rc != 0 && ::WSAGetLastError() != WSA_IO_PENDING) { return false; }
                const WSAEVENT events[] = {stopped(), ready()};
                const DWORD wait = rc == 0 ? WSA_WAIT_EVENT_0 + 1 :
                    ::WSAWaitForMultipleEvents(2, events, FALSE, waitMs < 0 ? WSA_INFINITE : static_cast<DWORD>(waitMs), FALSE);
                DWORD completed = 0;
                if (wait != WSA_WAIT_EVENT_0 + 1 || expired(deadline)) {
                    (void)::CancelIoEx(reinterpret_cast<HANDLE>(s), &operation);
                    // Cancellation completes asynchronously; retain the buffer
                    // and OVERLAPPED until the socket releases both.
                    (void)::WSAGetOverlappedResult(s, &operation, &completed, TRUE, &flags);
                    return false;
                }
                if (!::WSAGetOverlappedResult(s, &operation, &completed, FALSE, &flags) || completed == 0) { return false; }
                received += completed;
                #else
                pollfd readable[] = {{s, POLLIN, 0}, {stopped(), POLLIN, 0}};
                const auto ready = ::poll(readable, 2, static_cast<int>(waitMs));
                if (ready < 0 && errno == EINTR) { continue; }
                if (ready < 0 || expired(deadline) || readable[1].revents != 0) { return false; }
                if (ready == 0) { continue; }
                const ssize_t n = ::recv(s, data + received, size - received, 0); if (n < 0 && errno == EINTR) { continue; }
                if (n <= 0) { return false; }
                received += static_cast<size_t>(n);
                #endif
            }
            return true;
        }

    private:
        bool expired(ConnectionDeadline deadline) const { return cancelled() || ConnectionClock::now() >= deadline; }
        std::atomic<bool> cancelled_{false};
        #ifdef _WIN32
        HANDLE stopped_ = nullptr;
        HANDLE ready_ = nullptr;
        #else
        int pipe_[2]{-1, -1};
        #endif
    };

    CancellableSocketReader::CancellableSocketReader() : impl_{std::make_unique<Impl>()} {}
    CancellableSocketReader::~CancellableSocketReader() = default;
    void CancellableSocketReader::cancel() { impl_->cancel(); }
    void CancellableSocketReader::reset() { impl_->reset(); }
    bool CancellableSocketReader::cancelled() const { return impl_->cancelled(); }
    bool CancellableSocketReader::readAll(ConnectionSocket socket, uint8_t* data, size_t size, ConnectionDeadline deadline) {
        return impl_->readAll(socket, data, size, deadline);
    }
}

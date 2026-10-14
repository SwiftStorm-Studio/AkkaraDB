/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */

    uint16_t connectionListenerPort(const RaftTestSocket& listener) {
        sockaddr_in address{};
        #ifdef _WIN32
        int size = sizeof(address);
        #else
        socklen_t size = sizeof(address);
        #endif
        AKK_CLUSTER_CHECK(::getsockname(listener.handle, reinterpret_cast<sockaddr*>(&address), &size) == 0);
        return ntohs(address.sin_port);
    }

    detail::ConnectionAddress connectionLoopbackAddress(uint16_t port) {
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(port);
        AKK_CLUSTER_CHECK(::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr) == 1);
        detail::ConnectionAddress out;
        std::memcpy(out.bytes.data(), &address, sizeof(address));
        out.length = sizeof(address);
        out.family = AF_INET;
        out.socketType = SOCK_STREAM;
        out.protocol = IPPROTO_TCP;
        return out;
    }

    void testClusterResolverDeadlineAndBounds() {
        using Clock = detail::ConnectionClock;
        using Addresses = detail::DeadlineResolver::Addresses;
        auto started = std::make_shared<std::atomic<unsigned>>(0);
        auto completed = std::make_shared<std::atomic<unsigned>>(0);
        std::promise<void> release;
        auto gate = release.get_future().share();
        auto resolver = std::make_unique<detail::DeadlineResolver>(
            [gate, started, completed](const std::string&, uint16_t) {
                ++*started;
                gate.wait();
                ++*completed;
                return Addresses{connectionLoopbackAddress(1)};
            }, 2);
        const auto timedLookup = [&](const std::string& host) {
            const auto before = Clock::now();
            AKK_CLUSTER_CHECK(resolver->resolve(host, 1, before + std::chrono::milliseconds{60}).empty());
            AKK_CLUSTER_CHECK(Clock::now() - before < std::chrono::milliseconds{300});
        };
        timedLookup("slow-a");
        AKK_CLUSTER_CHECK(waitUntil([&] { return started->load() == 1; }));
        // Repeated timeouts coalesce with the outstanding lookup.
        for (int i = 0; i < 3; ++i) { timedLookup("slow-a"); }
        AKK_CLUSTER_CHECK(started->load() == 1);
        timedLookup("slow-b");
        AKK_CLUSTER_CHECK(waitUntil([&] { return started->load() == 2; }));
        timedLookup("capacity-full");
        AKK_CLUSTER_CHECK(started->load() == 2);

        std::atomic<bool> cancelled{false};
        auto waiter = std::async(std::launch::async, [&] {
            return resolver->resolve("slow-a", 1, Clock::now() + std::chrono::seconds{5}, [&] { return cancelled.load(); });
        });
        std::this_thread::sleep_for(std::chrono::milliseconds{40});
        const auto cancelAt = Clock::now();
        cancelled = true;
        AKK_CLUSTER_CHECK(waiter.wait_for(std::chrono::milliseconds{300}) == std::future_status::ready);
        AKK_CLUSTER_CHECK(waiter.get().empty());
        AKK_CLUSTER_CHECK(Clock::now() - cancelAt < std::chrono::milliseconds{300});

        const auto destroyAt = Clock::now();
        resolver.reset();
        AKK_CLUSTER_CHECK(Clock::now() - destroyAt < std::chrono::milliseconds{100});
        // The jobs still own their lookup inputs after the resolver is gone.
        release.set_value();
        AKK_CLUSTER_CHECK(waitUntil([&] { return completed->load() == 2; }));
    }

    void testClusterConnectionSharedDeadline() {
        using Clock = detail::ConnectionClock;
        using Addresses = detail::DeadlineResolver::Addresses;
        RaftTestSocket unusedListener;
        unusedListener.listen(0);
        const auto unusedPort = connectionListenerPort(unusedListener);
        unusedListener.close();
        detail::DeadlineResolver resolver{[unusedPort](const std::string&, uint16_t) {
            std::this_thread::sleep_for(std::chrono::milliseconds{100});
            return Addresses(3, connectionLoopbackAddress(unusedPort));
        }, 1};
        const auto before = Clock::now();
        const auto deadline = before + std::chrono::milliseconds{180};
        const auto socket = detail::connectWithDeadline(resolver, "multiple-addresses", unusedPort, deadline);
        AKK_CLUSTER_CHECK(socket == detail::INVALID_CONNECTION_SOCKET);
        AKK_CLUSTER_CHECK(Clock::now() - before < std::chrono::milliseconds{250});

        std::atomic<bool> cancelled{false};
        auto connecting = std::async(std::launch::async, [&] {
            return detail::connectWithDeadline("::1", unusedPort, Clock::now() + std::chrono::seconds{5},
                [&] { return cancelled.load(); });
        });
        std::this_thread::sleep_for(std::chrono::milliseconds{40});
        cancelled = true;
        AKK_CLUSTER_CHECK(connecting.wait_for(std::chrono::milliseconds{300}) == std::future_status::ready);
        AKK_CLUSTER_CHECK(connecting.get() == detail::INVALID_CONNECTION_SOCKET);

        auto lookups = std::make_shared<std::atomic<unsigned>>(0);
        detail::DeadlineResolver failing{[lookups](const std::string&, uint16_t) -> Addresses {
            ++*lookups;
            throw std::runtime_error("resolver failure");
        }, 1};
        AKK_CLUSTER_CHECK(failing.resolve("expired", 1, Clock::now()).empty());
        AKK_CLUSTER_CHECK(failing.resolve("cancelled", 1, Clock::now() + std::chrono::seconds{1}, [] { return true; }).empty());
        AKK_CLUSTER_CHECK(lookups->load() == 0);
        for (int i = 0; i < 2; ++i) {
            AKK_CLUSTER_CHECK(failing.resolve("failure", 1, Clock::now() + std::chrono::seconds{1}).empty());
        }
        AKK_CLUSTER_CHECK(lookups->load() == 2);
    }

    void testClusterConnectionAddressesAndShutdown() {
        using Clock = detail::ConnectionClock;
        using Addresses = detail::DeadlineResolver::Addresses;
        RaftTestSocket listener;
        listener.listen(0);
        const auto port = connectionListenerPort(listener);
        detail::DeadlineResolver resolver{[port](const std::string&, uint16_t) {
            return Addresses{connectionLoopbackAddress(0), connectionLoopbackAddress(port)};
        }, 1};
        {
            RaftTestSocket connection{detail::connectWithDeadline(resolver, "fallback", port,
                Clock::now() + std::chrono::seconds{1})};
            AKK_CLUSTER_CHECK(connection.handle != RaftTestSocket::INVALID);
        }
        auto ipv6Miss = connectionLoopbackAddress(port);
        sockaddr_in6 ipv6Address{};
        ipv6Address.sin6_family = AF_INET6;
        ipv6Address.sin6_port = htons(port);
        AKK_CLUSTER_CHECK(::inet_pton(AF_INET6, "::1", &ipv6Address.sin6_addr) == 1);
        std::memcpy(ipv6Miss.bytes.data(), &ipv6Address, sizeof(ipv6Address));
        ipv6Miss.length = sizeof(ipv6Address);
        ipv6Miss.family = AF_INET6;
        detail::DeadlineResolver groupedFamilies{[ipv6Miss, port](const std::string&, uint16_t) {
            return Addresses{ipv6Miss, ipv6Miss, connectionLoopbackAddress(port)};
        }, 1};
        {
            RaftTestSocket connection{detail::connectWithDeadline(groupedFamilies, "two-ipv6-before-ipv4", port,
                Clock::now() + std::chrono::milliseconds{180})};
            AKK_CLUSTER_CHECK(connection.handle != RaftTestSocket::INVALID);
        }
        for (const auto* host : {"127.0.0.1", "localhost"}) {
            RaftTestSocket connection{detail::connectWithDeadline(host, port, Clock::now() + std::chrono::seconds{1})};
            AKK_CLUSTER_CHECK(connection.handle != RaftTestSocket::INVALID);
        }
        detail::DeadlineResolver systemResolver;
        const auto ipv6 = systemResolver.resolve("::1", port, Clock::now() + std::chrono::seconds{1});
        AKK_CLUSTER_CHECK(!ipv6.empty() && ipv6.front().family == AF_INET6);
        AKK_CLUSTER_CHECK(detail::connectWithDeadline("127.0.0.1", port, Clock::now()) == detail::INVALID_CONNECTION_SOCKET);

        for (const bool secure : {false, true}) {
            for (int i = 0; i < 4; ++i) {
                RaftTestSocket silentListener;
                silentListener.listen(0);
                ClusterRuntimeOptions options;
                options.transportMode = secure ? TransportMode::SECURE : TransportMode::PLAIN;
                if (secure) {
                    options.secure.expectedPrimaryNodeId = 1;
                    options.secure.pinnedPeers = {{1, akkaradb::crypto::generateNodeIdentity().publicKey}};
                }
                auto client = ReplicationClient::create("localhost", connectionListenerPort(silentListener), 2,
                    [] { return uint64_t{0}; }, {}, options);
                client->start();
                if (i != 0) { std::this_thread::sleep_for(std::chrono::milliseconds{40}); }
                const auto closeAt = Clock::now();
                client->close();
                if (Clock::now() - closeAt >= std::chrono::milliseconds{350}) {
                    std::fprintf(stderr, "connection close: secure=%d iteration=%d elapsed=%lldms\n", secure, i,
                        static_cast<long long>(std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - closeAt).count()));
                }
                AKK_CLUSTER_CHECK(Clock::now() - closeAt < std::chrono::milliseconds{350});
                AKK_CLUSTER_CHECK(!client->connected());
            }
        }
        // Closing during retry backoff also wakes the worker immediately.
        RaftTestSocket offlineListener;
        offlineListener.listen(0);
        const auto offlinePort = connectionListenerPort(offlineListener);
        offlineListener.close();
        ClusterRuntimeOptions offlineOptions;
        offlineOptions.transportMode = TransportMode::PLAIN;
        auto offline = ReplicationClient::create("127.0.0.1", offlinePort, 2, [] { return uint64_t{0}; }, {}, offlineOptions);
        offline->start();
        std::this_thread::sleep_for(std::chrono::milliseconds{1050});
        const auto closeAt = Clock::now();
        offline->close();
        AKK_CLUSTER_CHECK(Clock::now() - closeAt < std::chrono::milliseconds{100});
    }

    void runClusterConnectionDeadlineTests() {
        testClusterResolverDeadlineAndBounds();
        testClusterConnectionSharedDeadline();
        testClusterConnectionAddressesAndShutdown();
    }

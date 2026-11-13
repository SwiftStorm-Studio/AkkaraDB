/*
 * Copyright (C) 2026 Swift Storm Studio
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
#include "throughput/LatencyMeasurement.hpp"

#include <iostream>
#include <limits>

namespace {
    void require(bool condition, const char* message) {
        if (!condition) { throw std::runtime_error(message); }
    }
    void near(double actual, double expected, const char* message) {
        require(std::abs(actual - expected) <= std::max(1.0, std::abs(expected)) * 1e-12, message);
    }
    template<class Function>
    void rejects(Function function) {
        try { function(); } catch (const std::invalid_argument&) { return; }
        throw std::runtime_error("invalid measurement input was accepted");
    }
    struct ManualClock {
        using duration = std::chrono::nanoseconds;
        using time_point = std::chrono::time_point<ManualClock, duration>;
        static inline int64_t ns = 0;
        static time_point now() { return time_point{duration{ns}}; }
    };
    struct ReturnedValue {
        int* destroyed;
        ~ReturnedValue() { ManualClock::ns += 20; ++*destroyed; }
    };
}

int main() {
    try {
        using namespace akkaradb::benchmark;
        std::vector<double> values;
        for (int i = 1000; i > 0; --i) { values.push_back(static_cast<double>(i)); }
        const auto metrics = summarize(values, 7000);
        near(metrics.meanNs, 7, "bulk ns/op");
        near(metrics.opsPerSecond, 1e9 / 7, "ops/s unit conversion");
        near(metrics.latencyMean, 500.5, "latency mean");
        near(metrics.latencyStddev, std::sqrt((1'000'000.0 - 1) / 12), "population standard deviation");
        require(metrics.min == 1 && metrics.max == 1000, "latency bounds");
        require(metrics.p50 == 500 && metrics.p90 == 900 && metrics.p99 == 990 && metrics.p999 == 999,
            "nearest-rank percentiles");
        std::vector<double> single{7};
        const auto one = summarize(single, 10);
        require(one.p50 == 7 && one.p999 == 7 && one.latencyStddev == 0, "single-sample boundary");
        rejects([&] { summarize({}, 1); });
        rejects([&] { summarize(single, 0); });
        rejects([&] { summarize(single, -1); });
        rejects([&] { summarize(single, std::numeric_limits<double>::infinity()); });
        for (double invalid : {-1.0, std::numeric_limits<double>::infinity(), std::numeric_limits<double>::quiet_NaN()}) {
            std::vector<double> bad{invalid};
            rejects([&] { summarize(bad, 1); });
        }

        int calls = 0, destroyed = 0, preparations = 0;
        const auto samples = collect<ManualClock>(5, 2,
            [&](size_t) { ++calls; ManualClock::ns += 10; return ReturnedValue{&destroyed}; },
            [](const ReturnedValue&) { ManualClock::ns += 3; return uint64_t{1}; },
            [&] { ++preparations; ManualClock::ns += 1000; });
        require(calls == 14 && destroyed == 14 && samples.checksum == 14, "warmup/measured calls and result disposal");
        require(preparations == 4, "pass preparation count");
        require(samples.bulkNs == 165, "bulk includes lookup, consumption and disposal, excludes preparation");
        require(samples.samples.size() == 5, "only measured calls are sampled");
        for (double sample : samples.samples) {
            require(sample == 33, "individual timing includes returned-value disposal and excludes preparation");
        }
        std::cout << "Latency measurement contract checks passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}

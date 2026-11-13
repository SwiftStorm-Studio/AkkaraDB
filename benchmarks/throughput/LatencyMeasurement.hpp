/*
 * Copyright (C) 2026 Swift Storm Studio
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <numeric>
#include <span>
#include <stdexcept>
#include <vector>

namespace akkaradb::benchmark {
    struct Metrics {
        double bulkNs, meanNs, opsPerSecond;
        double latencyMean, latencyStddev, min, p50, p90, p99, p999, max;
    };

    inline double percentile(std::span<const double> sorted, size_t permille) {
        if (sorted.empty() || permille == 0 || permille > 1000) {
            throw std::invalid_argument("invalid percentile input");
        }
        // Nearest rank without floating-point rounding or size*permille overflow.
        const size_t rank = (sorted.size() / 1000) * permille +
            ((sorted.size() % 1000) * permille + 999) / 1000;
        return sorted[rank - 1];
    }

    inline Metrics summarize(std::span<double> samples, double bulkNs) {
        if (samples.empty() || !std::isfinite(bulkNs) || bulkNs <= 0) {
            throw std::invalid_argument("bulk duration must be positive; increase iterations if below clock resolution");
        }
        for (double value : samples) {
            if (!std::isfinite(value) || value < 0) { throw std::invalid_argument("invalid latency sample"); }
        }
        const double count = static_cast<double>(samples.size());
        const double mean = std::accumulate(samples.begin(), samples.end(), 0.0) / count;
        double squaredDeviations = 0;
        for (double value : samples) { squaredDeviations += (value - mean) * (value - mean); }
        std::ranges::sort(samples);
        return {bulkNs, bulkNs / count, count * 1e9 / bulkNs,
            mean, std::sqrt(squaredDeviations / count), samples.front(),
            percentile(samples, 500), percentile(samples, 900), percentile(samples, 990),
            percentile(samples, 999), samples.back()};
    }

    struct Measurements {
        std::vector<double> samples;
        double bulkNs = 0;
        uint64_t checksum = 0;
    };

    template<class Clock = std::chrono::steady_clock, class Operation, class Observe, class Prepare>
    Measurements collect(int iterations, int warmup, Operation operation, Observe observe, Prepare prepare) {
        if (iterations <= 0 || warmup < 0) { throw std::invalid_argument("invalid measurement counts"); }
        Measurements result{std::vector<double>(static_cast<size_t>(iterations), 0.0)};
        const auto elapsed = [](auto start, auto end) {
            return std::chrono::duration<double, std::nano>(end - start).count();
        };
        const auto warm = [&] {
            for (int i = 0; i < warmup; ++i) { result.checksum += observe(operation(static_cast<size_t>(i))); }
        };
        warm();
        prepare();
        std::atomic_signal_fence(std::memory_order_seq_cst);
        const auto bulkStart = Clock::now();
        for (int i = 0; i < iterations; ++i) { result.checksum += observe(operation(static_cast<size_t>(i))); }
        std::atomic_signal_fence(std::memory_order_seq_cst);
        const auto bulkEnd = Clock::now();
        std::atomic_signal_fence(std::memory_order_seq_cst);
        result.bulkNs = elapsed(bulkStart, bulkEnd);
        prepare();
        warm();
        prepare();
        for (int i = 0; i < iterations; ++i) {
            std::atomic_signal_fence(std::memory_order_seq_cst);
            const auto start = Clock::now();
            // The full expression destroys returned buffers/pins before the end timestamp,
            // matching the result-consumption/disposal boundary of the bulk loop.
            const auto observed = observe(operation(static_cast<size_t>(i)));
            std::atomic_signal_fence(std::memory_order_seq_cst);
            const auto end = Clock::now();
            std::atomic_signal_fence(std::memory_order_seq_cst);
            result.samples[static_cast<size_t>(i)] = elapsed(start, end);
            result.checksum += observed;
        }
        prepare();
        return result;
    }
}

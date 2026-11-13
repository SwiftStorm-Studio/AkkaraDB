/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/AkkEngineWriteAdmission.inc
// Included inside AkkEngine::Impl by AkkEngineImpl.hpp.
[[nodiscard]] bool canUseParallelWriteAdmission() const noexcept {
    const bool supported = !walWriter && !blobManager && !clusterRuntime;
    const bool explicitSupported = !blobManager && !clusterRuntime;
    switch (opts.runtime.writeAdmission) {
        case AkkEngineOptions::WriteAdmissionMode::SERIAL: return false;
        case AkkEngineOptions::WriteAdmissionMode::PARALLEL: return explicitSupported;
        case AkkEngineOptions::WriteAdmissionMode::AUTO: return opts.runtime.relaxedConcurrentWrites && supported;
    }
    return false;
}

[[nodiscard]] bool usesKeySequenceOrder() const noexcept {
    return canUseParallelWriteAdmission() && opts.runtime.parallelWriteOrder == AkkEngineOptions::ParallelWriteOrderMode::KEY_SEQUENCE;
}

[[nodiscard]] std::mutex& keySequenceOrderMutex(uint64_t fp64, size_t keySize) noexcept {
    const uint64_t route = fp64 ^ (keySize * 0x9E3779B97F4A7C15ULL);
    return (*keySequenceOrderMu)[route & (KEY_SEQUENCE_ORDER_STRIPES - 1u)];
}

[[nodiscard]] std::vector<std::unique_lock<std::mutex>> lockKeySequenceOrderStripes(std::span<const BatchPutEntry> entries) {
    std::vector<size_t> stripes;
    stripes.reserve(entries.size());
    for (const auto& [key, value] : entries) {
        const uint64_t fp64 = key.empty() ? 0 : computeKeyFp64(key.data(), key.size());
        const uint64_t route = fp64 ^ (key.size() * 0x9E3779B97F4A7C15ULL);
        stripes.push_back(route & (KEY_SEQUENCE_ORDER_STRIPES - 1u));
    }
    std::ranges::sort(stripes);
    stripes.erase(std::ranges::unique(stripes).begin(), stripes.end());

    std::vector<std::unique_lock<std::mutex>> locks;
    locks.reserve(stripes.size());
    for (const size_t stripe : stripes) { locks.emplace_back((*keySequenceOrderMu)[stripe]); }
    return locks;
}

[[nodiscard]] bool memtableBackpressureActive() const noexcept {
    const uint32_t limit = opts.runtime.backpressure.maxMemtableImmutableTables;
    if (limit == 0 || !memtable) { return false; }
    return memtable->snapshot().immutableTables >= limit;
}

[[nodiscard]] bool sstBackpressureActive() const {
    const uint32_t limit = opts.runtime.backpressure.maxSstL0Files;
    if (limit == 0 || !sstManager) { return false; }
    const auto levels = sstManager->levelStats();
    for (const auto& level : levels) { if (level.level == 0) { return level.fileCount >= limit; } }
    return false;
}

void recordBackpressureWait(std::chrono::steady_clock::duration elapsed) const noexcept {
    const uint64_t micros = std::max<int64_t>(0, std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count());
    backpressureWaitMicrosTotal.fetch_add(micros, std::memory_order_relaxed);
    uint64_t observed = backpressureWaitMicrosMax.load(std::memory_order_relaxed);
    while (observed < micros && !backpressureWaitMicrosMax.compare_exchange_weak(
        observed,
        micros,
        std::memory_order_relaxed,
        std::memory_order_relaxed
    )) {}
}

[[noreturn]] void rejectBackpressuredWrite(const char* reason, bool timedOut) const {
    backpressureRejectedWrites.fetch_add(1, std::memory_order_relaxed);
    if (timedOut) { backpressureTimedOutWrites.fetch_add(1, std::memory_order_relaxed); }
    std::string message = "AkkEngine: write backpressure ";
    message += timedOut ? "timed out: " : "rejected write: ";
    message += reason;
    throw std::runtime_error(message);
}

void applyWriteBackpressure() const {
    if (!writeBackpressureEnabled) {
        if (closed.load(std::memory_order_acquire)) { throw std::runtime_error("AkkEngine: engine is closed"); }
        throwIfBackgroundFailed();
        if (memtable) { memtable->throwIfFlushFailed(); }
        return;
    }
    const auto started = std::chrono::steady_clock::now();
    const auto timeout = std::chrono::milliseconds{opts.runtime.backpressure.timeoutMs};
    bool waited = false;
    bool countedMemtable = false;
    bool countedSst = false;
    while (true) {
        if (closed.load(std::memory_order_acquire)) {
            if (waited) { recordBackpressureWait(std::chrono::steady_clock::now() - started); }
            throw std::runtime_error("AkkEngine: engine is closed");
        }
        throwIfBackgroundFailed();
        if (memtable) { memtable->throwIfFlushFailed(); }

        AkkEngineOptions::BackpressureMode mode;
        const char* reason = nullptr;
        if (memtableBackpressureActive()) {
            mode = opts.runtime.backpressure.memtableFlushBacklog;
            reason = "memtable immutable-table backlog";
            if (!countedMemtable) {
                backpressureMemtableStalls.fetch_add(1, std::memory_order_relaxed);
                countedMemtable = true;
            }
        }
        else if (sstBackpressureActive()) {
            mode = opts.runtime.backpressure.sstCompactionBacklog;
            reason = "sst L0 compaction backlog";
            if (!countedSst) {
                backpressureSstStalls.fetch_add(1, std::memory_order_relaxed);
                countedSst = true;
            }
        }
        else {
            if (waited) { recordBackpressureWait(std::chrono::steady_clock::now() - started); }
            return;
        }

        if (mode == AkkEngineOptions::BackpressureMode::FAIL_FAST) { rejectBackpressuredWrite(reason, false); }
        if (!waited) {
            backpressureBlockedWrites.fetch_add(1, std::memory_order_relaxed);
            waited = true;
        }

        const auto elapsed = std::chrono::steady_clock::now() - started;
        if (opts.runtime.backpressure.timeoutMs != 0 && elapsed >= timeout) {
            recordBackpressureWait(elapsed);
            rejectBackpressuredWrite(reason, true);
        }

        auto wait = std::chrono::microseconds{std::max<uint32_t>(1, opts.runtime.backpressure.waitMicros)};
        if (opts.runtime.backpressure.timeoutMs != 0) {
            const auto remaining = std::chrono::duration_cast<std::chrono::microseconds>(timeout - elapsed);
            wait = std::min(wait, std::max(std::chrono::microseconds{1}, remaining));
        }
        std::this_thread::sleep_for(wait);
    }
}

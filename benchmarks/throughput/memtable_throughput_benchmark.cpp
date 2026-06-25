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

// benchmarks/throughput/memtableThroughputBenchmark.cpp
#include "TestErrorHandlers.hpp"

/*
 * Sharded MemTable throughput benchmark (size sweep).
 *
 * Measures:
 *  - PUT throughput (unique keys)
 *  - GET throughput (existing keys)
 *
 * Runs one backend with configurable shard/flush settings.
 *
 * Usage:
 *   akkaradbMemtableThroughputBenchmark [opsPerCase]
 *       [--writers=N] [--shards=N] [--auto-cap=N]
 *       [--prehash] [--backend=skiplist|bptree|art]
 *       [--compare-flush-modes]
 *
 * Default:
 *   opsPerCase = 200000
 *   writers      = 16
 *   shards       = 0 (auto)
 */

#include "akk/engine/memtable/MemTable.hpp"
#include "akk/engine/memtable/ARTMemTable.hpp"
#include "akk/engine/memtable/BPTreeMemTable.hpp"
#include "akk/engine/memtable/SkipListMemTable.hpp"
#include "akk/core/record/KeyFingerprint.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <format>
#include <limits>
#include <numeric>
#include <new>
#include <optional>
#include <string>
#include <thread>
#include <string_view>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#include <malloc.h>
#endif

using Clock = std::chrono::steady_clock;
using namespace akkaradb::engine;
using namespace akkaradb::engine::memtable;
using namespace akkaradb::core;

namespace {
    constexpr bool kDistributionMode = false;
    constexpr uint32_t kLatencySampleMask = 0x3F; // sample 1 / 64 ops to reduce benchmark perturbation
    constexpr size_t kScanSampleWindow = 32; // amortize clock resolution/overhead for iterator next()
    constexpr uint64_t kAutoFlushDisabledThreshold = 0;
    constexpr size_t kKeyChunkSize = 1'000'000;
    constexpr uint64_t kDiskBenchmarkFileSizeBytes = 64ULL * 1024ULL * 1024ULL;
    constexpr uint32_t kDiskBenchmarkRuns = 5;
    constexpr uint32_t kDiskBenchmarkDurationSeconds = 5;

    struct CaseSpec {
        int keySize;
        int valueSize;
    };

    struct LatencyPercentiles {
        double p50Us = 0.0;
        double p90Us = 0.0;
        double p99Us = 0.0;
        double p999Us = 0.0;
        uint32_t sampleCount = 0;
    };

    enum class BackendKind {
        SkipList,
        BPTree,
        ART
    };

    enum class FlushMode {
        NoFlush,
        DefaultThreshold
    };

    struct DiskMetricStats {
        double throughputMeanMiBPerSec = 0.0;
        double throughputMedianMiBPerSec = 0.0;
        double iopsMean = 0.0;
        double iopsMedian = 0.0;
    };

    struct DiskRunResult {
        double throughputMiBPerSec = 0.0;
        double iops = 0.0;
    };

    struct DiskBenchmarkSummary {
        bool available = false;
        std::string tempFilePath;
        std::string error;
        uint64_t fileSizeBytes = 0;
        uint32_t runs = 0;
        DiskMetricStats seq1mQ8T1Read;
        DiskMetricStats seq1mQ8T1Write;
        DiskMetricStats seq1mQ1T1Read;
        DiskMetricStats seq1mQ1T1Write;
        DiskMetricStats rnd4kQ32T1Read;
        DiskMetricStats rnd4kQ32T1Write;
        DiskMetricStats rnd4kQ1T1Read;
        DiskMetricStats rnd4kQ1T1Write;
    };

    struct SystemProfile {
        std::string cpuName = "unknown";
        uint32_t cpuPhysicalCores = 0;
        uint32_t cpuLogicalCores = 0;
        std::string osName = "unknown";
        std::string osArchitecture = "unknown";
        uint64_t ramTotalBytes = 0;
        uint32_t ramModuleCount = 0;
        double ramSpeedMeanMtps = 0.0;
        std::string ramSpeedListMtps;
        std::string benchmarkDrive = "unknown";
        std::string ssdModel = "unknown";
        std::string ssdMediaType = "unknown";
        DiskBenchmarkSummary disk;
    };

    struct ThroughputResult {
        double putOpsPerSec;
        double getOpsPerSec;
        double scanOpsPerSec;
        double putMs;
        double putFlushOpsPerSec;
        double putFlushMs;
        double getMs;
        double scanMs;
        double flushMs;
        uint64_t approxBytes;
        uint64_t flushesCompleted;
        uint64_t flushRecordsSeen;
        LatencyPercentiles putLatency;
        LatencyPercentiles getLatency;
        LatencyPercentiles scanLatency;
    };

    struct NumericStats {
        double mean = 0.0;
        double median = 0.0;
        double stddev = 0.0;
        double cov = 0.0;
    };

    struct ResultStats {
        NumericStats putOpsPerSec;
        NumericStats putFlushOpsPerSec;
        NumericStats getOpsPerSec;
        NumericStats scanOpsPerSec;
        NumericStats putMs;
        NumericStats putFlushMs;
        NumericStats getMs;
        NumericStats scanMs;
        NumericStats flushMs;
        NumericStats approxBytes;
        NumericStats flushesCompleted;
        NumericStats flushRecordsSeen;
        NumericStats putLatencyP50Us;
        NumericStats putLatencyP90Us;
        NumericStats putLatencyP99Us;
        NumericStats putLatencyP999Us;
        NumericStats getLatencyP50Us;
        NumericStats getLatencyP90Us;
        NumericStats getLatencyP99Us;
        NumericStats getLatencyP999Us;
        NumericStats scanLatencyP50Us;
        NumericStats scanLatencyP90Us;
        NumericStats scanLatencyP99Us;
        NumericStats scanLatencyP999Us;
    };

    struct CaseReport {
        CaseSpec spec{};
        ThroughputResult averaged{};
        ResultStats stats{};
    };

    struct FlushModeReport {
        FlushMode flushMode = FlushMode::NoFlush;
        std::vector<CaseReport> reports;
    };

    struct BackendReport {
        BackendKind backend = BackendKind::BPTree;
        std::vector<FlushModeReport> modeReports;
    };

    struct BenchConfig {
        int opsPerCase = 2500000;
        int repeats = 10;
        int writerThreads = 16;
        uint32_t requestedShards = 64;
        uint32_t autoShardCountCap = 128;
        bool usePrehash = false;
        bool compareFlushModes = false;
        BackendKind backend = BackendKind::BPTree;
        std::string outputPath;
    };

    [[nodiscard]] static uint32_t nextPow2Clamped(uint64_t n, uint32_t minValue, uint32_t maxValue) {
        uint32_t p = 1;
        while (p < n && p < maxValue) {
            p <<= 1;
        }
        if (p < minValue) {
            p = minValue;
        }
        if (p > maxValue) {
            p = maxValue;
        }
        return p;
    }

    [[nodiscard]] static uint32_t resolveShardCount(
        size_t requested,
        size_t expectedConcurrentWriters,
        size_t autoCap
    ) {
        if (requested == 1) {
            return 1;
        }
        if (requested > 1) {
            return nextPow2Clamped(static_cast<uint64_t>(requested), 2, 256);
        }

        const uint32_t effectiveCap = nextPow2Clamped(static_cast<uint64_t>(autoCap == 0 ? 128 : autoCap), 2, 256);
        const size_t n = expectedConcurrentWriters > 0
            ? expectedConcurrentWriters
            : std::max<size_t>(2, std::thread::hardware_concurrency());
        const uint64_t target = n <= 1 ? 1ULL : static_cast<uint64_t>(n) * 4ULL;
        return nextPow2Clamped(target, 2, effectiveCap);
    }

    [[nodiscard]] static int resolveWriterThreads(int requested) {
        if (requested > 0) {
            return requested;
        }
        const unsigned hw = std::thread::hardware_concurrency();
        return static_cast<int>(hw == 0 ? 1u : hw);
    }

    static std::span<const uint8_t> asU8(const std::string& s) {
        return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
    }

    [[nodiscard]] static std::string formatBytes(uint64_t bytes) {
        constexpr double kKiB = 1024.0;
        constexpr double kMiB = 1024.0 * 1024.0;
        constexpr double kGiB = 1024.0 * 1024.0 * 1024.0;
        constexpr double kTiB = 1024.0 * 1024.0 * 1024.0 * 1024.0;

        if (bytes >= static_cast<uint64_t>(kTiB)) {
            return std::format("{:.2f} TiB", static_cast<double>(bytes) / kTiB);
        }
        if (bytes >= static_cast<uint64_t>(kGiB)) {
            return std::format("{:.2f} GiB", static_cast<double>(bytes) / kGiB);
        }
        if (bytes >= static_cast<uint64_t>(kMiB)) {
            return std::format("{:.2f} MiB", static_cast<double>(bytes) / kMiB);
        }
        if (bytes >= static_cast<uint64_t>(kKiB)) {
            return std::format("{:.2f} KiB", static_cast<double>(bytes) / kKiB);
        }
        return std::format("{} B", bytes);
    }

    [[nodiscard]] static const char* flushModeName(FlushMode mode) noexcept {
        switch (mode) {
            case FlushMode::NoFlush: return "disabled";
            case FlushMode::DefaultThreshold: return "default_threshold";
        }
        return "unknown";
    }

    [[nodiscard]] static const char* backendName(BackendKind backend) noexcept {
        switch (backend) {
            case BackendKind::SkipList: return "skiplist";
            case BackendKind::BPTree: return "bptree";
            case BackendKind::ART: return "art";
        }
        return "unknown";
    }

    [[nodiscard]] static std::string trimAscii(std::string_view text) {
        size_t start = 0;
        while (start < text.size() && static_cast<unsigned char>(text[start]) <= 0x20U) {
            ++start;
        }
        size_t end = text.size();
        while (end > start && static_cast<unsigned char>(text[end - 1]) <= 0x20U) {
            --end;
        }
        return std::string{text.substr(start, end - start)};
    }

    static void accumulateLatency(LatencyPercentiles& dst, const LatencyPercentiles& src) {
        dst.p50Us += src.p50Us;
        dst.p90Us += src.p90Us;
        dst.p99Us += src.p99Us;
        dst.p999Us += src.p999Us;
        dst.sampleCount += src.sampleCount;
    }

    static void divideLatency(LatencyPercentiles& value, double divisor) {
        value.p50Us /= divisor;
        value.p90Us /= divisor;
        value.p99Us /= divisor;
        value.p999Us /= divisor;
        value.sampleCount = static_cast<uint32_t>(std::llround(static_cast<double>(value.sampleCount) / divisor));
    }

    [[nodiscard]] static ThroughputResult averageResults(const std::vector<ThroughputResult>& runs) {
        ThroughputResult out{};
        if (runs.empty()) {
            return out;
        }

        for (const auto& run : runs) {
            out.putOpsPerSec += run.putOpsPerSec;
            out.putFlushOpsPerSec += run.putFlushOpsPerSec;
            out.getOpsPerSec += run.getOpsPerSec;
            out.scanOpsPerSec += run.scanOpsPerSec;
            out.putMs += run.putMs;
            out.putFlushMs += run.putFlushMs;
            out.getMs += run.getMs;
            out.scanMs += run.scanMs;
            out.flushMs += run.flushMs;
            out.approxBytes += run.approxBytes;
            out.flushesCompleted += run.flushesCompleted;
            out.flushRecordsSeen += run.flushRecordsSeen;
            accumulateLatency(out.putLatency, run.putLatency);
            accumulateLatency(out.getLatency, run.getLatency);
            accumulateLatency(out.scanLatency, run.scanLatency);
        }

        const double divisor = static_cast<double>(runs.size());
        out.putOpsPerSec /= divisor;
        out.putFlushOpsPerSec /= divisor;
        out.getOpsPerSec /= divisor;
        out.scanOpsPerSec /= divisor;
        out.putMs /= divisor;
        out.putFlushMs /= divisor;
        out.getMs /= divisor;
        out.scanMs /= divisor;
        out.flushMs /= divisor;
        out.approxBytes = static_cast<uint64_t>(std::llround(static_cast<double>(out.approxBytes) / divisor));
        out.flushesCompleted = static_cast<uint64_t>(std::llround(static_cast<double>(out.flushesCompleted) / divisor));
        out.flushRecordsSeen = static_cast<uint64_t>(std::llround(static_cast<double>(out.flushRecordsSeen) / divisor));
        divideLatency(out.putLatency, divisor);
        divideLatency(out.getLatency, divisor);
        divideLatency(out.scanLatency, divisor);
        return out;
    }

    [[nodiscard]] static NumericStats computeNumericStats(std::vector<double> values) {
        NumericStats out{};
        if (values.empty()) {
            return out;
        }

        const double sum = std::accumulate(values.begin(), values.end(), 0.0);
        out.mean = sum / static_cast<double>(values.size());

        std::sort(values.begin(), values.end());
        const size_t mid = values.size() / 2;
        if ((values.size() & 1U) == 0U) {
            out.median = (values[mid - 1] + values[mid]) * 0.5;
        } else {
            out.median = values[mid];
        }

        double variance = 0.0;
        for (const double value : values) {
            const double delta = value - out.mean;
            variance += delta * delta;
        }
        variance /= static_cast<double>(values.size());
        out.stddev = std::sqrt(variance);
        out.cov = out.mean != 0.0 ? (out.stddev / out.mean) : 0.0;
        return out;
    }

    [[nodiscard]] static DiskMetricStats computeDiskMetricStats(
        const std::vector<double>& throughputSamples,
        const std::vector<double>& iopsSamples
    ) {
        DiskMetricStats out{};
        out.throughputMeanMiBPerSec = computeNumericStats(throughputSamples).mean;
        out.throughputMedianMiBPerSec = computeNumericStats(throughputSamples).median;
        out.iopsMean = computeNumericStats(iopsSamples).mean;
        out.iopsMedian = computeNumericStats(iopsSamples).median;
        return out;
    }

    [[nodiscard]] static std::string commandOutput(const std::string& command) {
        std::string out;
#ifdef _WIN32
        FILE* pipe = _popen(command.c_str(), "rt");
#else
        FILE* pipe = popen(command.c_str(), "r");
#endif
        if (pipe == nullptr) {
            return out;
        }
        std::array<char, 4096> buffer{};
        while (std::fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
            out.append(buffer.data());
        }
#ifdef _WIN32
        _pclose(pipe);
#else
        pclose(pipe);
#endif
        return out;
    }

    [[nodiscard]] static std::optional<uint64_t> parseU64(const std::string& text) {
        if (text.empty()) {
            return std::nullopt;
        }
        char* end = nullptr;
        const unsigned long long value = std::strtoull(text.c_str(), &end, 10);
        if (end == text.c_str() || (end != nullptr && *end != '\0')) {
            return std::nullopt;
        }
        return static_cast<uint64_t>(value);
    }

    [[nodiscard]] static std::optional<double> parseDouble(const std::string& text) {
        if (text.empty()) {
            return std::nullopt;
        }
        char* end = nullptr;
        const double value = std::strtod(text.c_str(), &end);
        if (end == text.c_str() || (end != nullptr && *end != '\0')) {
            return std::nullopt;
        }
        return value;
    }

    [[nodiscard]] static std::string quoteCommandArg(const std::filesystem::path& path) {
        std::string text = path.string();
        size_t pos = 0;
        while ((pos = text.find('"', pos)) != std::string::npos) {
            text.replace(pos, 1, "\"\"");
            pos += 2;
        }
        return '"' + text + '"';
    }

    [[nodiscard]] static std::vector<std::string> splitPipeColumns(const std::string& line) {
        std::vector<std::string> columns;
        size_t start = 0;
        while (start <= line.size()) {
            const size_t sep = line.find('|', start);
            columns.push_back(trimAscii(line.substr(start, sep == std::string::npos ? std::string::npos : sep - start)));
            if (sep == std::string::npos) {
                break;
            }
            start = sep + 1;
        }
        return columns;
    }

    [[nodiscard]] static std::optional<DiskRunResult> parseDiskSpdSectionTotals(
        const std::string& output,
        std::string_view sectionTitle
    ) {
        const std::string marker = std::string(sectionTitle) + "\n";
        const size_t sectionPos = output.find(marker);
        if (sectionPos == std::string::npos) {
            return std::nullopt;
        }
        const size_t totalPos = output.find("total:", sectionPos);
        if (totalPos == std::string::npos) {
            return std::nullopt;
        }
        const size_t lineEnd = output.find('\n', totalPos);
        const std::string totalLine = trimAscii(output.substr(totalPos, lineEnd == std::string::npos ? std::string::npos : lineEnd - totalPos));
        const std::vector<std::string> columns = splitPipeColumns(totalLine);
        if (columns.size() < 5) {
            return std::nullopt;
        }
        const std::optional<double> throughput = parseDouble(columns[2]);
        const std::optional<double> iops = parseDouble(columns[3]);
        if (!throughput.has_value() || !iops.has_value()) {
            return std::nullopt;
        }
        return DiskRunResult{
            .throughputMiBPerSec = *throughput,
            .iops = *iops
        };
    }

    [[nodiscard]] static std::string escapePowerShellSingleQuoted(std::string value) {
        size_t pos = 0;
        while ((pos = value.find('\'', pos)) != std::string::npos) {
            value.replace(pos, 1, "''");
            pos += 2;
        }
        return value;
    }

    [[nodiscard]] static std::string benchmarkDriveFromPath(const std::filesystem::path& path) {
        const std::filesystem::path absolute = std::filesystem::absolute(path);
        const std::string root = absolute.root_name().string();
        if (root.size() >= 2 && root[1] == ':') {
            return std::string{static_cast<char>(std::toupper(static_cast<unsigned char>(root[0])))};
        }
        return "C";
    }

#ifdef _WIN32
    struct AlignedBuffer {
        void* ptr = nullptr;
        size_t size = 0;

        AlignedBuffer() = default;
        explicit AlignedBuffer(size_t n) : ptr(_aligned_malloc(n, 4096)), size(n) {
            if (ptr == nullptr) {
                throw std::bad_alloc{};
            }
        }
        AlignedBuffer(const AlignedBuffer&) = delete;
        AlignedBuffer& operator=(const AlignedBuffer&) = delete;
        AlignedBuffer(AlignedBuffer&& other) noexcept : ptr(other.ptr), size(other.size) {
            other.ptr = nullptr;
            other.size = 0;
        }
        AlignedBuffer& operator=(AlignedBuffer&& other) noexcept {
            if (this != &other) {
                if (ptr != nullptr) {
                    _aligned_free(ptr);
                }
                ptr = other.ptr;
                size = other.size;
                other.ptr = nullptr;
                other.size = 0;
            }
            return *this;
        }
        ~AlignedBuffer() {
            if (ptr != nullptr) {
                _aligned_free(ptr);
            }
        }
    };

    struct IoSlot {
        OVERLAPPED overlapped{};
        HANDLE eventHandle = nullptr;
        AlignedBuffer buffer;
        bool active = false;

        explicit IoSlot(size_t blockSize) : eventHandle(CreateEventW(nullptr, TRUE, FALSE, nullptr)), buffer(blockSize) {
            if (eventHandle == nullptr) {
                throw std::runtime_error("CreateEventW failed for disk benchmark");
            }
            std::memset(&overlapped, 0, sizeof(overlapped));
            overlapped.hEvent = eventHandle;
            std::memset(buffer.ptr, 0xA5, blockSize);
        }
        IoSlot(const IoSlot&) = delete;
        IoSlot& operator=(const IoSlot&) = delete;
        IoSlot(IoSlot&& other) noexcept
            : overlapped(other.overlapped),
              eventHandle(other.eventHandle),
              buffer(std::move(other.buffer)),
              active(other.active) {
            other.eventHandle = nullptr;
            other.active = false;
            std::memset(&other.overlapped, 0, sizeof(other.overlapped));
        }
        IoSlot& operator=(IoSlot&& other) noexcept {
            if (this != &other) {
                if (eventHandle != nullptr) {
                    CloseHandle(eventHandle);
                }
                overlapped = other.overlapped;
                eventHandle = other.eventHandle;
                buffer = std::move(other.buffer);
                active = other.active;
                other.eventHandle = nullptr;
                other.active = false;
                std::memset(&other.overlapped, 0, sizeof(other.overlapped));
            }
            return *this;
        }
        ~IoSlot() {
            if (eventHandle != nullptr) {
                CloseHandle(eventHandle);
            }
        }
    };

    [[nodiscard]] static uint64_t xorshift64(uint64_t& state) noexcept {
        state ^= (state << 13);
        state ^= (state >> 7);
        state ^= (state << 17);
        return state;
    }

    static void assignOverlappedOffset(OVERLAPPED& overlapped, uint64_t offset) noexcept {
        overlapped.Offset = static_cast<DWORD>(offset & 0xFFFFFFFFULL);
        overlapped.OffsetHigh = static_cast<DWORD>((offset >> 32) & 0xFFFFFFFFULL);
    }

    [[nodiscard]] static std::wstring widenPath(const std::filesystem::path& path) {
        return path.wstring();
    }

    static void ensureBenchmarkFileSize(const std::filesystem::path& path, uint64_t fileSizeBytes) {
        HANDLE handle = CreateFileW(
            widenPath(path).c_str(),
            GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            nullptr,
            CREATE_ALWAYS,
            FILE_ATTRIBUTE_NORMAL,
            nullptr
        );
        if (handle == INVALID_HANDLE_VALUE) {
            throw std::runtime_error("CreateFileW failed while preparing disk benchmark file");
        }
        LARGE_INTEGER size{};
        size.QuadPart = static_cast<LONGLONG>(fileSizeBytes);
        const BOOL seekOk = SetFilePointerEx(handle, size, nullptr, FILE_BEGIN);
        const BOOL sizeOk = seekOk ? SetEndOfFile(handle) : FALSE;
        CloseHandle(handle);
        if (!seekOk || !sizeOk) {
            throw std::runtime_error("Failed to size disk benchmark file");
        }
    }

    [[nodiscard]] static DiskRunResult runWindowsDiskIoCase(
        const std::filesystem::path& path,
        bool writeMode,
        bool randomAccess,
        uint32_t queueDepth,
        size_t blockSize,
        uint64_t fileSizeBytes,
        uint64_t seed
    ) {
        const DWORD desiredAccess = writeMode ? (GENERIC_READ | GENERIC_WRITE) : GENERIC_READ;
        DWORD flags = FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING;
        flags |= randomAccess ? FILE_FLAG_RANDOM_ACCESS : FILE_FLAG_SEQUENTIAL_SCAN;
        if (writeMode) {
            flags |= FILE_FLAG_WRITE_THROUGH;
        }

        HANDLE handle = CreateFileW(
            widenPath(path).c_str(),
            desiredAccess,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            nullptr,
            OPEN_EXISTING,
            flags,
            nullptr
        );
        if (handle == INVALID_HANDLE_VALUE) {
            throw std::runtime_error("CreateFileW failed while opening disk benchmark file");
        }

        const uint64_t totalOps = fileSizeBytes / static_cast<uint64_t>(blockSize);
        if (totalOps == 0) {
            CloseHandle(handle);
            throw std::runtime_error("Disk benchmark file size is smaller than block size");
        }

        std::vector<IoSlot> slots;
        slots.reserve(queueDepth);
        for (uint32_t i = 0; i < queueDepth; ++i) {
            slots.emplace_back(blockSize);
        }

        std::vector<HANDLE> events;
        events.reserve(queueDepth);
        for (auto& slot : slots) {
            events.push_back(slot.eventHandle);
        }

        uint64_t submitted = 0;
        uint64_t completed = 0;
        uint64_t state = seed ^ 0x9E3779B97F4A7C15ULL;

        auto nextOffset = [&](uint64_t opIndex) mutable -> uint64_t {
            if (!randomAccess) {
                return (opIndex % totalOps) * static_cast<uint64_t>(blockSize);
            }
            const uint64_t blockIndex = xorshift64(state) % totalOps;
            return blockIndex * static_cast<uint64_t>(blockSize);
        };

        const auto submitOp = [&](IoSlot& slot, uint64_t opIndex) {
            slot.active = true;
            ResetEvent(slot.eventHandle);
            std::memset(&slot.overlapped, 0, sizeof(slot.overlapped));
            slot.overlapped.hEvent = slot.eventHandle;
            assignOverlappedOffset(slot.overlapped, nextOffset(opIndex));
            DWORD transferred = 0;
            const BOOL ok = writeMode
                ? WriteFile(handle, slot.buffer.ptr, static_cast<DWORD>(blockSize), &transferred, &slot.overlapped)
                : ReadFile(handle, slot.buffer.ptr, static_cast<DWORD>(blockSize), &transferred, &slot.overlapped);
            if (!ok) {
                const DWORD error = GetLastError();
                if (error != ERROR_IO_PENDING) {
                    throw std::runtime_error(std::format("Disk benchmark I/O submit failed (winerr={})", error));
                }
            }
        };

        const uint32_t initial = static_cast<uint32_t>(std::min<uint64_t>(queueDepth, totalOps));
        for (uint32_t i = 0; i < initial; ++i) {
            submitOp(slots[static_cast<size_t>(i)], submitted++);
        }

        const auto t0 = Clock::now();
        while (completed < totalOps) {
            const DWORD wait = WaitForMultipleObjects(static_cast<DWORD>(events.size()), events.data(), FALSE, INFINITE);
            if (wait < WAIT_OBJECT_0 || wait >= WAIT_OBJECT_0 + events.size()) {
                CloseHandle(handle);
                throw std::runtime_error("WaitForMultipleObjects failed during disk benchmark");
            }
            const size_t index = static_cast<size_t>(wait - WAIT_OBJECT_0);
            IoSlot& slot = slots[index];
            DWORD transferred = 0;
            if (!GetOverlappedResult(handle, &slot.overlapped, &transferred, FALSE)) {
                const DWORD error = GetLastError();
                CloseHandle(handle);
                throw std::runtime_error(std::format("Disk benchmark I/O completion failed (winerr={})", error));
            }
            if (transferred != blockSize) {
                CloseHandle(handle);
                throw std::runtime_error("Disk benchmark transferred byte count mismatch");
            }
            ++completed;
            slot.active = false;
            if (submitted < totalOps) {
                submitOp(slot, submitted++);
            } else {
                ResetEvent(slot.eventHandle);
            }
        }
        if (writeMode) {
            FlushFileBuffers(handle);
        }
        const double seconds = std::chrono::duration<double>(Clock::now() - t0).count();
        CloseHandle(handle);

        DiskRunResult out{};
        if (seconds > 0.0) {
            out.throughputMiBPerSec = (static_cast<double>(fileSizeBytes) / (1024.0 * 1024.0)) / seconds;
            out.iops = static_cast<double>(totalOps) / seconds;
        }
        return out;
    }
#endif

    [[nodiscard]] static SystemProfile collectSystemProfile(
        const std::filesystem::path& outputPath,
        const std::filesystem::path& diskSpdPath
    ) {
        SystemProfile profile{};
        profile.benchmarkDrive = benchmarkDriveFromPath(outputPath);

#ifdef _WIN32
        const std::string driveLetter = escapePowerShellSingleQuoted(profile.benchmarkDrive);
        const std::string ps = std::format(
            "powershell -NoProfile -ExecutionPolicy Bypass -Command "
            "\"$ErrorActionPreference='SilentlyContinue'; "
            "[Console]::OutputEncoding=[System.Text.Encoding]::UTF8; "
            "$drive='{}'; "
            "$cpu=Get-CimInstance Win32_Processor | Select-Object -First 1 Name,NumberOfCores,NumberOfLogicalProcessors; "
            "$os=Get-CimInstance Win32_OperatingSystem | Select-Object -First 1 Caption,OSArchitecture; "
            "$mem=@(Get-CimInstance Win32_PhysicalMemory | Where-Object {{$_.Capacity -gt 0}}); "
            "$part=Get-Partition -DriveLetter $drive -ErrorAction SilentlyContinue | Select-Object -First 1; "
            "$disk=$null; "
            "if($part) {{$disk=Get-Disk -Number $part.DiskNumber -ErrorAction SilentlyContinue | Select-Object -First 1 FriendlyName,Model,MediaType;}} "
            "if(-not $disk) {{$disk=Get-CimInstance Win32_DiskDrive | Select-Object -First 1 Model,MediaType;}} "
            "$ramTotal=($mem | Measure-Object -Property Capacity -Sum).Sum; "
            "$ramCount=@($mem).Count; "
            "$ramSpeeds=@($mem | Where-Object {{$_.Speed -gt 0}} | ForEach-Object {{$_.Speed}}); "
            "$ramSpeedMean=0; "
            "if($ramSpeeds.Count -gt 0) {{$ramSpeedMean=[Math]::Round((($ramSpeeds | Measure-Object -Average).Average),2);}} "
            "$ramSpeedList=($ramSpeeds -join ';'); "
            "Write-Output ('cpu_name=' + $cpu.Name); "
            "Write-Output ('cpu_cores=' + $cpu.NumberOfCores); "
            "Write-Output ('cpu_logical=' + $cpu.NumberOfLogicalProcessors); "
            "Write-Output ('os_name=' + $os.Caption); "
            "Write-Output ('os_arch=' + $os.OSArchitecture); "
            "Write-Output ('ram_total=' + $ramTotal); "
            "Write-Output ('ram_modules=' + $ramCount); "
            "Write-Output ('ram_speed_mean=' + $ramSpeedMean); "
            "Write-Output ('ram_speed_list=' + $ramSpeedList); "
            "if($disk.Model) {{ Write-Output ('disk_model=' + $disk.Model); }} "
            "elseif($disk.FriendlyName) {{ Write-Output ('disk_model=' + $disk.FriendlyName); }} "
            "else {{ Write-Output 'disk_model='; }} "
            "Write-Output ('disk_media=' + $disk.MediaType);\"",
            driveLetter
        );

        const std::string raw = commandOutput(ps);
        size_t start = 0;
        while (start < raw.size()) {
            const size_t end = raw.find('\n', start);
            const std::string line = trimAscii(raw.substr(start, end == std::string::npos ? std::string::npos : end - start));
            if (!line.empty()) {
                const size_t eq = line.find('=');
                const std::string key = eq == std::string::npos ? line : line.substr(0, eq);
                const std::string value = eq == std::string::npos ? std::string{} : trimAscii(line.substr(eq + 1));
                if (key == "cpu_name") {
                    profile.cpuName = value.empty() ? profile.cpuName : value;
                } else if (key == "cpu_cores") {
                    profile.cpuPhysicalCores = static_cast<uint32_t>(parseU64(value).value_or(0));
                } else if (key == "cpu_logical") {
                    profile.cpuLogicalCores = static_cast<uint32_t>(parseU64(value).value_or(0));
                } else if (key == "os_name") {
                    profile.osName = value.empty() ? profile.osName : value;
                } else if (key == "os_arch") {
                    profile.osArchitecture = value.empty() ? profile.osArchitecture : value;
                } else if (key == "ram_total") {
                    profile.ramTotalBytes = parseU64(value).value_or(0);
                } else if (key == "ram_modules") {
                    profile.ramModuleCount = static_cast<uint32_t>(parseU64(value).value_or(0));
                } else if (key == "ram_speed_mean") {
                    profile.ramSpeedMeanMtps = parseDouble(value).value_or(0.0);
                } else if (key == "ram_speed_list") {
                    profile.ramSpeedListMtps = value;
                } else if (key == "disk_model") {
                    profile.ssdModel = value.empty() ? profile.ssdModel : value;
                } else if (key == "disk_media") {
                    profile.ssdMediaType = value.empty() ? profile.ssdMediaType : value;
                }
            }
            if (end == std::string::npos) {
                break;
            }
            start = end + 1;
        }

        try {
            if (!std::filesystem::exists(diskSpdPath)) {
                throw std::runtime_error("diskspd.exe was not found next to the benchmark executable");
            }

            const std::filesystem::path tempFile = std::filesystem::absolute(outputPath).parent_path() / "memtable_disk_probe.tmp";
            profile.disk.tempFilePath = tempFile.string();
            profile.disk.fileSizeBytes = kDiskBenchmarkFileSizeBytes;
            profile.disk.runs = kDiskBenchmarkRuns;

            auto runMetric = [&](bool writeMode, bool randomAccess, uint32_t queueDepth, size_t blockSize) {
                std::vector<double> throughputs;
                std::vector<double> iops;
                throughputs.reserve(kDiskBenchmarkRuns);
                iops.reserve(kDiskBenchmarkRuns);

                const std::string sectionTitle = writeMode ? "Write IO" : "Read IO";
                const std::string blockArg = blockSize == 1024 * 1024 ? "1M" : "4K";
                const std::string accessHint = randomAccess ? "-fr" : "-fs";
                const std::string randomArg = randomAccess ? "-r " : "";
                const std::string writeArg = writeMode ? "-w100" : "-w0";

                for (uint32_t run = 0; run < kDiskBenchmarkRuns; ++run) {
                    std::error_code removeEc;
                    std::filesystem::remove(tempFile, removeEc);
                    const std::string command = std::format(
                        "{} -c64M -W0 -C0 -d{} -Sh -L -b{} -o{} -t1 {} {} {} {} 2>&1",
                        quoteCommandArg(diskSpdPath),
                        kDiskBenchmarkDurationSeconds,
                        blockArg,
                        queueDepth,
                        accessHint,
                        randomArg,
                        writeArg,
                        quoteCommandArg(tempFile)
                    );
                    const std::string output = commandOutput(command);
                    const std::optional<DiskRunResult> parsed = parseDiskSpdSectionTotals(output, sectionTitle);
                    if (!parsed.has_value()) {
                        throw std::runtime_error("Failed to parse DiskSpd output for " + sectionTitle);
                    }
                    throughputs.push_back(parsed->throughputMiBPerSec);
                    iops.push_back(parsed->iops);
                }
                return computeDiskMetricStats(throughputs, iops);
            };

            profile.disk.seq1mQ8T1Write = runMetric(true, false, 8, 1024 * 1024);
            profile.disk.seq1mQ1T1Write = runMetric(true, false, 1, 1024 * 1024);
            profile.disk.rnd4kQ32T1Write = runMetric(true, true, 32, 4 * 1024);
            profile.disk.rnd4kQ1T1Write = runMetric(true, true, 1, 4 * 1024);
            profile.disk.seq1mQ8T1Read = runMetric(false, false, 8, 1024 * 1024);
            profile.disk.seq1mQ1T1Read = runMetric(false, false, 1, 1024 * 1024);
            profile.disk.rnd4kQ32T1Read = runMetric(false, true, 32, 4 * 1024);
            profile.disk.rnd4kQ1T1Read = runMetric(false, true, 1, 4 * 1024);
            profile.disk.available = true;
            std::error_code ignoreEc;
            std::filesystem::remove(tempFile, ignoreEc);
        } catch (const std::exception& ex) {
            profile.disk.error = ex.what();
        }
#endif
        return profile;
    }

    [[nodiscard]] static ResultStats computeResultStats(const std::vector<ThroughputResult>& runs) {
        ResultStats stats{};
        if (runs.empty()) {
            return stats;
        }

        std::vector<double> putOps;
        std::vector<double> putFlushOps;
        std::vector<double> getOps;
        std::vector<double> scanOps;
        std::vector<double> putMs;
        std::vector<double> putFlushMs;
        std::vector<double> getMs;
        std::vector<double> scanMs;
        std::vector<double> flushMs;
        std::vector<double> approxBytes;
        std::vector<double> flushesCompleted;
        std::vector<double> flushRecordsSeen;
        std::vector<double> putP50;
        std::vector<double> putP90;
        std::vector<double> putP99;
        std::vector<double> putP999;
        std::vector<double> getP50;
        std::vector<double> getP90;
        std::vector<double> getP99;
        std::vector<double> getP999;
        std::vector<double> scanP50;
        std::vector<double> scanP90;
        std::vector<double> scanP99;
        std::vector<double> scanP999;

        putOps.reserve(runs.size());
        putFlushOps.reserve(runs.size());
        getOps.reserve(runs.size());
        scanOps.reserve(runs.size());
        putMs.reserve(runs.size());
        putFlushMs.reserve(runs.size());
        getMs.reserve(runs.size());
        scanMs.reserve(runs.size());
        flushMs.reserve(runs.size());
        approxBytes.reserve(runs.size());
        flushesCompleted.reserve(runs.size());
        flushRecordsSeen.reserve(runs.size());
        putP50.reserve(runs.size());
        putP90.reserve(runs.size());
        putP99.reserve(runs.size());
        putP999.reserve(runs.size());
        getP50.reserve(runs.size());
        getP90.reserve(runs.size());
        getP99.reserve(runs.size());
        getP999.reserve(runs.size());
        scanP50.reserve(runs.size());
        scanP90.reserve(runs.size());
        scanP99.reserve(runs.size());
        scanP999.reserve(runs.size());

        for (const auto& run : runs) {
            putOps.push_back(run.putOpsPerSec);
            putFlushOps.push_back(run.putFlushOpsPerSec);
            getOps.push_back(run.getOpsPerSec);
            scanOps.push_back(run.scanOpsPerSec);
            putMs.push_back(run.putMs);
            putFlushMs.push_back(run.putFlushMs);
            getMs.push_back(run.getMs);
            scanMs.push_back(run.scanMs);
            flushMs.push_back(run.flushMs);
            approxBytes.push_back(static_cast<double>(run.approxBytes));
            flushesCompleted.push_back(static_cast<double>(run.flushesCompleted));
            flushRecordsSeen.push_back(static_cast<double>(run.flushRecordsSeen));
            putP50.push_back(run.putLatency.p50Us);
            putP90.push_back(run.putLatency.p90Us);
            putP99.push_back(run.putLatency.p99Us);
            putP999.push_back(run.putLatency.p999Us);
            getP50.push_back(run.getLatency.p50Us);
            getP90.push_back(run.getLatency.p90Us);
            getP99.push_back(run.getLatency.p99Us);
            getP999.push_back(run.getLatency.p999Us);
            scanP50.push_back(run.scanLatency.p50Us);
            scanP90.push_back(run.scanLatency.p90Us);
            scanP99.push_back(run.scanLatency.p99Us);
            scanP999.push_back(run.scanLatency.p999Us);
        }

        stats.putOpsPerSec = computeNumericStats(std::move(putOps));
        stats.putFlushOpsPerSec = computeNumericStats(std::move(putFlushOps));
        stats.getOpsPerSec = computeNumericStats(std::move(getOps));
        stats.scanOpsPerSec = computeNumericStats(std::move(scanOps));
        stats.putMs = computeNumericStats(std::move(putMs));
        stats.putFlushMs = computeNumericStats(std::move(putFlushMs));
        stats.getMs = computeNumericStats(std::move(getMs));
        stats.scanMs = computeNumericStats(std::move(scanMs));
        stats.flushMs = computeNumericStats(std::move(flushMs));
        stats.approxBytes = computeNumericStats(std::move(approxBytes));
        stats.flushesCompleted = computeNumericStats(std::move(flushesCompleted));
        stats.flushRecordsSeen = computeNumericStats(std::move(flushRecordsSeen));
        stats.putLatencyP50Us = computeNumericStats(std::move(putP50));
        stats.putLatencyP90Us = computeNumericStats(std::move(putP90));
        stats.putLatencyP99Us = computeNumericStats(std::move(putP99));
        stats.putLatencyP999Us = computeNumericStats(std::move(putP999));
        stats.getLatencyP50Us = computeNumericStats(std::move(getP50));
        stats.getLatencyP90Us = computeNumericStats(std::move(getP90));
        stats.getLatencyP99Us = computeNumericStats(std::move(getP99));
        stats.getLatencyP999Us = computeNumericStats(std::move(getP999));
        stats.scanLatencyP50Us = computeNumericStats(std::move(scanP50));
        stats.scanLatencyP90Us = computeNumericStats(std::move(scanP90));
        stats.scanLatencyP99Us = computeNumericStats(std::move(scanP99));
        stats.scanLatencyP999Us = computeNumericStats(std::move(scanP999));
        return stats;
    }

    static void writeCsvField(std::ofstream& out, const std::string& value) {
        bool needsQuotes = false;
        for (const char ch : value) {
            if (ch == ',' || ch == '"' || ch == '\n' || ch == '\r') {
                needsQuotes = true;
                break;
            }
        }
        if (!needsQuotes) {
            out << value;
            return;
        }
        out << '"';
        for (const char ch : value) {
            if (ch == '"') {
                out << "\"\"";
            } else {
                out << ch;
            }
        }
        out << '"';
    }

    static void writeStatColumns(std::ofstream& out, const NumericStats& stats) {
        out << ',' << stats.mean
            << ',' << stats.median
            << ',' << stats.stddev
            << ',' << stats.cov;
    }

    static void writeDiskMetricColumns(std::ofstream& out, const DiskMetricStats& stats) {
        out << ',' << stats.throughputMeanMiBPerSec
            << ',' << stats.throughputMedianMiBPerSec
            << ',' << stats.iopsMean
            << ',' << stats.iopsMedian;
    }

    static void writeReportsCsv(
        const std::string& path,
        const BenchConfig& config,
        int effectiveWriters,
        uint32_t shardCount,
        const SystemProfile& profile,
        const std::vector<BackendReport>& backendReports
    ) {
        if (path.empty()) {
            return;
        }

        std::ofstream out(path, std::ios::out | std::ios::trunc);
        if (!out) {
            throw std::runtime_error("Failed to open benchmark output file: " + path);
        }

        out << "backend,flush_mode,ops_per_case,repeats,key_size,value_size,shards,writers,prehash,"
               "cpu_name,cpu_physical_cores,cpu_logical_cores,ram_total_bytes,ram_module_count,ram_speed_mean_mtps,ram_speed_list_mtps,os_name,os_architecture,benchmark_drive,ssd_model,ssd_media_type,"
               "disk_probe_available,disk_probe_error,disk_probe_file_size_bytes,disk_probe_runs,disk_probe_temp_path,"
               "seq1m_q8t1_read_mibps_mean,seq1m_q8t1_read_mibps_median,seq1m_q8t1_read_iops_mean,seq1m_q8t1_read_iops_median,"
               "seq1m_q8t1_write_mibps_mean,seq1m_q8t1_write_mibps_median,seq1m_q8t1_write_iops_mean,seq1m_q8t1_write_iops_median,"
               "seq1m_q1t1_read_mibps_mean,seq1m_q1t1_read_mibps_median,seq1m_q1t1_read_iops_mean,seq1m_q1t1_read_iops_median,"
               "seq1m_q1t1_write_mibps_mean,seq1m_q1t1_write_mibps_median,seq1m_q1t1_write_iops_mean,seq1m_q1t1_write_iops_median,"
               "rnd4k_q32t1_read_mibps_mean,rnd4k_q32t1_read_mibps_median,rnd4k_q32t1_read_iops_mean,rnd4k_q32t1_read_iops_median,"
               "rnd4k_q32t1_write_mibps_mean,rnd4k_q32t1_write_mibps_median,rnd4k_q32t1_write_iops_mean,rnd4k_q32t1_write_iops_median,"
               "rnd4k_q1t1_read_mibps_mean,rnd4k_q1t1_read_mibps_median,rnd4k_q1t1_read_iops_mean,rnd4k_q1t1_read_iops_median,"
               "rnd4k_q1t1_write_mibps_mean,rnd4k_q1t1_write_mibps_median,rnd4k_q1t1_write_iops_mean,rnd4k_q1t1_write_iops_median,"
               "put_ops_s,put_ms,put_flush_ops_s,put_flush_ms,get_ops_s,get_ms,scan_ops_s,scan_ms,flush_ms,mem_bytes,flushes_completed,flush_records_seen,"
               "put_smp,get_smp,scan_smp,"
               "put_p50_us,put_p90_us,put_p99_us,put_p999_us,"
               "get_p50_us,get_p90_us,get_p99_us,get_p999_us,"
               "scan_p50_us,scan_p90_us,scan_p99_us,scan_p999_us,"
               "put_ops_s_mean,put_ops_s_median,put_ops_s_stddev,put_ops_s_cov,"
               "put_flush_ops_s_mean,put_flush_ops_s_median,put_flush_ops_s_stddev,put_flush_ops_s_cov,"
               "get_ops_s_mean,get_ops_s_median,get_ops_s_stddev,get_ops_s_cov,"
               "scan_ops_s_mean,scan_ops_s_median,scan_ops_s_stddev,scan_ops_s_cov,"
               "put_ms_mean,put_ms_median,put_ms_stddev,put_ms_cov,"
               "put_flush_ms_mean,put_flush_ms_median,put_flush_ms_stddev,put_flush_ms_cov,"
               "get_ms_mean,get_ms_median,get_ms_stddev,get_ms_cov,"
               "scan_ms_mean,scan_ms_median,scan_ms_stddev,scan_ms_cov,"
               "flush_ms_mean,flush_ms_median,flush_ms_stddev,flush_ms_cov,"
               "mem_bytes_mean,mem_bytes_median,mem_bytes_stddev,mem_bytes_cov,"
               "flushes_completed_mean,flushes_completed_median,flushes_completed_stddev,flushes_completed_cov,"
               "flush_records_seen_mean,flush_records_seen_median,flush_records_seen_stddev,flush_records_seen_cov,"
               "put_p50_us_mean,put_p50_us_median,put_p50_us_stddev,put_p50_us_cov,"
               "put_p90_us_mean,put_p90_us_median,put_p90_us_stddev,put_p90_us_cov,"
               "put_p99_us_mean,put_p99_us_median,put_p99_us_stddev,put_p99_us_cov,"
               "put_p999_us_mean,put_p999_us_median,put_p999_us_stddev,put_p999_us_cov,"
               "get_p50_us_mean,get_p50_us_median,get_p50_us_stddev,get_p50_us_cov,"
               "get_p90_us_mean,get_p90_us_median,get_p90_us_stddev,get_p90_us_cov,"
               "get_p99_us_mean,get_p99_us_median,get_p99_us_stddev,get_p99_us_cov,"
               "get_p999_us_mean,get_p999_us_median,get_p999_us_stddev,get_p999_us_cov,"
               "scan_p50_us_mean,scan_p50_us_median,scan_p50_us_stddev,scan_p50_us_cov,"
               "scan_p90_us_mean,scan_p90_us_median,scan_p90_us_stddev,scan_p90_us_cov,"
               "scan_p99_us_mean,scan_p99_us_median,scan_p99_us_stddev,scan_p99_us_cov,"
               "scan_p999_us_mean,scan_p999_us_median,scan_p999_us_stddev,scan_p999_us_cov\n";

        for (const auto& backendReport : backendReports) {
            for (const auto& modeReport : backendReport.modeReports) {
                for (const auto& report : modeReport.reports) {
                    writeCsvField(out, backendName(backendReport.backend));
                    out << ',';
                    writeCsvField(out, flushModeName(modeReport.flushMode));
                    out << ',' << config.opsPerCase
                        << ',' << config.repeats
                        << ',' << report.spec.keySize
                        << ',' << report.spec.valueSize
                        << ',' << shardCount
                        << ',' << effectiveWriters
                        << ',' << (config.usePrehash ? 1 : 0)
                        << ',';
                    writeCsvField(out, profile.cpuName);
                    out << ',' << profile.cpuPhysicalCores
                        << ',' << profile.cpuLogicalCores
                        << ',' << profile.ramTotalBytes
                        << ',' << profile.ramModuleCount
                        << ',' << profile.ramSpeedMeanMtps
                        << ',';
                    writeCsvField(out, profile.ramSpeedListMtps);
                    out << ',';
                    writeCsvField(out, profile.osName);
                    out << ',';
                    writeCsvField(out, profile.osArchitecture);
                    out << ',';
                    writeCsvField(out, profile.benchmarkDrive);
                    out << ',';
                    writeCsvField(out, profile.ssdModel);
                    out << ',';
                    writeCsvField(out, profile.ssdMediaType);
                    out << ',' << (profile.disk.available ? 1 : 0)
                        << ',';
                    writeCsvField(out, profile.disk.error);
                    out << ',' << profile.disk.fileSizeBytes
                        << ',' << profile.disk.runs
                        << ',';
                    writeCsvField(out, profile.disk.tempFilePath);
                    writeDiskMetricColumns(out, profile.disk.seq1mQ8T1Read);
                    writeDiskMetricColumns(out, profile.disk.seq1mQ8T1Write);
                    writeDiskMetricColumns(out, profile.disk.seq1mQ1T1Read);
                    writeDiskMetricColumns(out, profile.disk.seq1mQ1T1Write);
                    writeDiskMetricColumns(out, profile.disk.rnd4kQ32T1Read);
                    writeDiskMetricColumns(out, profile.disk.rnd4kQ32T1Write);
                    writeDiskMetricColumns(out, profile.disk.rnd4kQ1T1Read);
                    writeDiskMetricColumns(out, profile.disk.rnd4kQ1T1Write);
                    out << ',' << report.averaged.putOpsPerSec
                        << ',' << report.averaged.putMs
                        << ',' << report.averaged.putFlushOpsPerSec
                        << ',' << report.averaged.putFlushMs
                        << ',' << report.averaged.getOpsPerSec
                        << ',' << report.averaged.getMs
                        << ',' << report.averaged.scanOpsPerSec
                        << ',' << report.averaged.scanMs
                        << ',' << report.averaged.flushMs
                        << ',' << report.averaged.approxBytes
                        << ',' << report.averaged.flushesCompleted
                        << ',' << report.averaged.flushRecordsSeen
                        << ',' << report.averaged.putLatency.sampleCount
                        << ',' << report.averaged.getLatency.sampleCount
                        << ',' << report.averaged.scanLatency.sampleCount
                        << ',' << report.averaged.putLatency.p50Us
                        << ',' << report.averaged.putLatency.p90Us
                        << ',' << report.averaged.putLatency.p99Us
                        << ',' << report.averaged.putLatency.p999Us
                        << ',' << report.averaged.getLatency.p50Us
                        << ',' << report.averaged.getLatency.p90Us
                        << ',' << report.averaged.getLatency.p99Us
                        << ',' << report.averaged.getLatency.p999Us
                        << ',' << report.averaged.scanLatency.p50Us
                        << ',' << report.averaged.scanLatency.p90Us
                        << ',' << report.averaged.scanLatency.p99Us
                        << ',' << report.averaged.scanLatency.p999Us;

                    writeStatColumns(out, report.stats.putOpsPerSec);
                    writeStatColumns(out, report.stats.putFlushOpsPerSec);
                    writeStatColumns(out, report.stats.getOpsPerSec);
                    writeStatColumns(out, report.stats.scanOpsPerSec);
                    writeStatColumns(out, report.stats.putMs);
                    writeStatColumns(out, report.stats.putFlushMs);
                    writeStatColumns(out, report.stats.getMs);
                    writeStatColumns(out, report.stats.scanMs);
                    writeStatColumns(out, report.stats.flushMs);
                    writeStatColumns(out, report.stats.approxBytes);
                    writeStatColumns(out, report.stats.flushesCompleted);
                    writeStatColumns(out, report.stats.flushRecordsSeen);
                    writeStatColumns(out, report.stats.putLatencyP50Us);
                    writeStatColumns(out, report.stats.putLatencyP90Us);
                    writeStatColumns(out, report.stats.putLatencyP99Us);
                    writeStatColumns(out, report.stats.putLatencyP999Us);
                    writeStatColumns(out, report.stats.getLatencyP50Us);
                    writeStatColumns(out, report.stats.getLatencyP90Us);
                    writeStatColumns(out, report.stats.getLatencyP99Us);
                    writeStatColumns(out, report.stats.getLatencyP999Us);
                    writeStatColumns(out, report.stats.scanLatencyP50Us);
                    writeStatColumns(out, report.stats.scanLatencyP90Us);
                    writeStatColumns(out, report.stats.scanLatencyP99Us);
                    writeStatColumns(out, report.stats.scanLatencyP999Us);
                    out << '\n';
                }
            }
        }
    }

    [[nodiscard]] static double payloadMibPerSec(size_t bytesPerOp, int ops, double ms) {
        if (ms <= 0.0) {
            return 0.0;
        }
        const double totalMib = static_cast<double>(bytesPerOp) * static_cast<double>(ops) / (1024.0 * 1024.0);
        return totalMib * 1000.0 / ms;
    }

    [[nodiscard]] static double quantileFromSorted(
        const std::vector<uint32_t>& sortedNs,
        double q
    ) {
        if (sortedNs.empty()) {
            return 0.0;
        }
        const double qClamped = std::clamp(q, 0.0, 1.0);
        const size_t idx = static_cast<size_t>(
            qClamped * static_cast<double>(sortedNs.size() - 1)
        );
        return static_cast<double>(sortedNs[idx]) / 1000.0;
    }

    [[nodiscard]] static LatencyPercentiles buildPercentiles(std::vector<uint32_t>& samplesNs) {
        LatencyPercentiles out{};
        if (samplesNs.empty()) {
            return out;
        }

        std::sort(samplesNs.begin(), samplesNs.end());
        out.sampleCount = static_cast<uint32_t>(samplesNs.size());
        out.p50Us = quantileFromSorted(samplesNs, 0.50);
        out.p90Us = quantileFromSorted(samplesNs, 0.90);
        out.p99Us = quantileFromSorted(samplesNs, 0.99);
        out.p999Us = quantileFromSorted(samplesNs, 0.999);
        return out;
    }

    static void fillFixedBytes(std::string& out, int size, uint64_t seed) {
        out.resize(static_cast<size_t>(size));
        uint64_t x = seed ^ 0x9e3779b97f4a7c15ULL;
        for (int i = 0; i < size; ++i) {
            x ^= (x << 13);
            x ^= (x >> 7);
            x ^= (x << 17);
            out[static_cast<size_t>(i)] = static_cast<char>(x & 0xFFU);
        }

        // Make benchmark keys injective for sizes >= 8 by embedding the seed in the suffix.
        const int uniqueBytes = std::min(size, 8);
        for (int i = 0; i < uniqueBytes; ++i) {
            out[static_cast<size_t>(size - uniqueBytes + i)] = static_cast<char>((seed >> (i * 8)) & 0xFFU);
        }
    }

    [[nodiscard]] static std::string makeFixedBytes(int size, uint64_t seed) {
        std::string out;
        fillFixedBytes(out, size, seed);
        return out;
    }

    static void generateKeyChunk(
        int keySize,
        size_t beginIndex,
        size_t count,
        bool usePrehash,
        std::vector<std::string>& keys,
        std::vector<uint64_t>& keyFp64,
        std::vector<uint64_t>& keyMk
    ) {
        keys.clear();
        keys.reserve(count);
        if (usePrehash) {
            keyFp64.clear();
            keyMk.clear();
            keyFp64.reserve(count);
            keyMk.reserve(count);
        }

        std::string key;
        for (size_t i = 0; i < count; ++i) {
            fillFixedBytes(key, keySize, static_cast<uint64_t>(beginIndex + i) + 1);
            keys.emplace_back(key);
            if (usePrehash) {
                const auto keyBytes = asU8(keys.back());
                keyFp64.push_back(computeKeyFp64(keyBytes));
                keyMk.push_back(buildMiniKey(keyBytes));
            }
        }
    }

    static memtable::MemTable::Options makeOptions(
        const BenchConfig& config,
        int effectiveWriters,
        FlushMode flushMode,
        std::atomic<uint64_t>* flushRecordsSeen
    ) {
        memtable::MemTable::Options opts;
        opts.shardCount = config.requestedShards;
        opts.expectedConcurrentWriters = static_cast<size_t>(effectiveWriters);
        opts.autoShardCountCap = config.autoShardCountCap;
        opts.thresholdBytesPerShard = flushMode == FlushMode::NoFlush
            ? static_cast<size_t>(kAutoFlushDisabledThreshold)
            : memtable::MemTable::Options{}.thresholdBytesPerShard;
        opts.onFlush = [flushRecordsSeen](std::span<const memtable::MemTable::RecordView> records) {
            if (flushRecordsSeen) {
                flushRecordsSeen->fetch_add(static_cast<uint64_t>(records.size()), std::memory_order_relaxed);
            }
        };
        if (config.backend == BackendKind::BPTree) {
            opts.backendFactory = []() {
                return std::make_unique<BPTreeMemTable>();
            };
        } else if (config.backend == BackendKind::ART) {
            opts.backendFactory = []() {
                return std::make_unique<ARTMemTable>();
            };
        } else {
            opts.backendFactory = []() {
                return std::make_unique<SkipListMemTable>();
            };
        }
        return opts;
    }

    static void runPutParallel(
        memtable::MemTable& memtable,
        const std::vector<std::string>& keys,
        const std::string& value,
        const std::vector<uint64_t>* keyFp64,
        const std::vector<uint64_t>* keyMk,
        int writerThreads,
        std::vector<uint32_t>* latencySamplesNs
    ) {
        if (latencySamplesNs) {
            latencySamplesNs->clear();
        }

        if (writerThreads <= 1) {
            if (latencySamplesNs) {
                latencySamplesNs->reserve((keys.size() + kLatencySampleMask) / (kLatencySampleMask + 1));
            }
            for (size_t i = 0; i < keys.size(); ++i) {
                const bool doSample = ((static_cast<uint32_t>(i) & kLatencySampleMask) == 0);
                const auto t0 = doSample ? Clock::now() : Clock::time_point{};
                const uint64_t seq = memtable.nextSeq();
                memtable.put(
                    asU8(keys[i]),
                    asU8(value),
                    seq,
                    0,
                    keyFp64 ? (*keyFp64)[i] : 0ULL,
                    keyMk ? (*keyMk)[i] : 0ULL
                );
                if (doSample && latencySamplesNs) {
                    const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
                    latencySamplesNs->push_back(static_cast<uint32_t>(std::min<int64_t>(dt, INT32_MAX)));
                }
            }
            return;
        }

        std::vector<std::thread> threads;
        threads.reserve(static_cast<size_t>(writerThreads));
        std::vector<std::vector<uint32_t>> localSamples(static_cast<size_t>(writerThreads));
        for (auto& v : localSamples) {
            v.reserve((keys.size() / static_cast<size_t>(writerThreads) + kLatencySampleMask) / (kLatencySampleMask + 1));
        }

        for (int tid = 0; tid < writerThreads; ++tid) {
            threads.emplace_back([&, tid]() {
                auto& samples = localSamples[static_cast<size_t>(tid)];
                const size_t stride = static_cast<size_t>(writerThreads);
                const size_t first = static_cast<size_t>(tid);
                const size_t opCount = first < keys.size() ? ((keys.size() - first + stride - 1) / stride) : 0;
                const uint64_t seqBase = memtable.reserveSeq(static_cast<uint64_t>(opCount));
                uint64_t seqOffset = 0;
                for (size_t i = first; i < keys.size(); i += stride) {
                    const bool doSample = ((static_cast<uint32_t>(i) & kLatencySampleMask) == 0);
                    const auto t0 = doSample ? Clock::now() : Clock::time_point{};
                    const uint64_t seq = seqBase + seqOffset++;
                    memtable.put(
                        asU8(keys[i]),
                        asU8(value),
                        seq,
                        0,
                        keyFp64 ? (*keyFp64)[i] : 0ULL,
                        keyMk ? (*keyMk)[i] : 0ULL
                    );
                    if (doSample) {
                        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
                        samples.push_back(static_cast<uint32_t>(std::min<int64_t>(dt, INT32_MAX)));
                    }
                }
            });
        }

        for (auto& th : threads) {
            th.join();
        }

        if (latencySamplesNs) {
            size_t total = 0;
            for (const auto& v : localSamples) {
                total += v.size();
            }
            latencySamplesNs->reserve(total);
            for (auto& v : localSamples) {
                latencySamplesNs->insert(latencySamplesNs->end(), v.begin(), v.end());
            }
        }
    }

    static void runGetParallel(
        memtable::MemTable& memtable,
        const std::vector<std::string>& keys,
        const std::vector<uint64_t>* keyFp64,
        uint64_t snapshot,
        int readerThreads,
        std::vector<uint32_t>* latencySamplesNs
    ) {
        if (latencySamplesNs) {
            latencySamplesNs->clear();
        }

        if (readerThreads <= 1) {
            RecordView out;
            if (latencySamplesNs) {
                latencySamplesNs->reserve((keys.size() + kLatencySampleMask) / (kLatencySampleMask + 1));
            }
            for (size_t i = 0; i < keys.size(); ++i) {
                const bool doSample = ((static_cast<uint32_t>(i) & kLatencySampleMask) == 0);
                const auto t0 = doSample ? Clock::now() : Clock::time_point{};
                if (!memtable.get(asU8(keys[i]), snapshot, &out, keyFp64 ? (*keyFp64)[i] : 0ULL)) {
                    std::fprintf(stderr, "GET miss at i=%zu\n", i);
                    std::exit(3);
                }
                if (doSample && latencySamplesNs) {
                    const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
                    latencySamplesNs->push_back(static_cast<uint32_t>(std::min<int64_t>(dt, INT32_MAX)));
                }
            }
            return;
        }

        std::vector<std::thread> threads;
        threads.reserve(static_cast<size_t>(readerThreads));
        std::vector<std::vector<uint32_t>> localSamples(static_cast<size_t>(readerThreads));
        for (auto& v : localSamples) {
            v.reserve((keys.size() / static_cast<size_t>(readerThreads) + kLatencySampleMask) / (kLatencySampleMask + 1));
        }

        for (int tid = 0; tid < readerThreads; ++tid) {
            threads.emplace_back([&, tid]() {
                RecordView out;
                auto& samples = localSamples[static_cast<size_t>(tid)];
                for (size_t i = static_cast<size_t>(tid); i < keys.size(); i += static_cast<size_t>(readerThreads)) {
                    const bool doSample = ((static_cast<uint32_t>(i) & kLatencySampleMask) == 0);
                    const auto t0 = doSample ? Clock::now() : Clock::time_point{};
                    if (!memtable.get(asU8(keys[i]), snapshot, &out, keyFp64 ? (*keyFp64)[i] : 0ULL)) {
                        std::fprintf(stderr, "GET miss at i=%zu (tid=%d)\n", i, tid);
                        std::exit(3);
                    }
                    if (doSample) {
                        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
                        samples.push_back(static_cast<uint32_t>(std::min<int64_t>(dt, INT32_MAX)));
                    }
                }
            });
        }

        for (auto& th : threads) {
            th.join();
        }

        if (latencySamplesNs) {
            size_t total = 0;
            for (const auto& v : localSamples) {
                total += v.size();
            }
            latencySamplesNs->reserve(total);
            for (auto& v : localSamples) {
                latencySamplesNs->insert(latencySamplesNs->end(), v.begin(), v.end());
            }
        }
    }

    static double runScanSingle(
        memtable::MemTable& memtable,
        uint64_t snapshot,
        size_t expectedRecords,
        std::vector<uint32_t>* latencySamplesNs
    ) {
        if (latencySamplesNs) {
            latencySamplesNs->clear();
            latencySamplesNs->reserve((expectedRecords + kLatencySampleMask) / (kLatencySampleMask + 1));
        }

        memtable::MemTable::KeyRange fullRange{};
        size_t scanned = 0;
        const auto scanT0 = Clock::now();
        auto it = memtable.iterator(fullRange, snapshot);
        while (it.hasNext()) {
            const bool doSample = ((static_cast<uint32_t>(scanned) & kLatencySampleMask) == 0);
            if (doSample && latencySamplesNs) {
                const auto t0 = Clock::now();
                size_t windowCount = 0;
                while (windowCount < kScanSampleWindow && it.hasNext()) {
                    const auto rec = it.next();
                    if (!rec.has_value()) {
                        std::fprintf(stderr, "SCAN iterator returned nullopt before end (scanned=%zu)\n", scanned);
                        std::exit(4);
                    }
                    ++windowCount;
                    ++scanned;
                }
                const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
                const int64_t perRecordNs = windowCount > 0 ? (dt / static_cast<int64_t>(windowCount)) : 0;
                latencySamplesNs->push_back(static_cast<uint32_t>(std::min<int64_t>(perRecordNs, INT32_MAX)));
                continue;
            }

            const auto rec = it.next();
            if (!rec.has_value()) {
                std::fprintf(stderr, "SCAN iterator returned nullopt before end (scanned=%zu)\n", scanned);
                std::exit(4);
            }
            ++scanned;
        }
        const auto scanMs = std::chrono::duration<double, std::milli>(Clock::now() - scanT0).count();

        if (scanned != expectedRecords) {
            std::fprintf(stderr, "SCAN count mismatch: expected=%zu actual=%zu\n", expectedRecords, scanned);
            std::exit(5);
        }
        if (scanMs <= 0.0) {
            return 0.0;
        }
        return static_cast<double>(scanned) * 1000.0 / scanMs;
    }

    static ThroughputResult runCase(
        const CaseSpec spec,
        int opsPerCase,
        int writerThreads,
        const BenchConfig& config,
        FlushMode flushMode
    ) {
        const int warmupOps = std::min(opsPerCase, 100000);

        const std::string value = makeFixedBytes(spec.valueSize, 0xA11CEULL);
        std::vector<std::string> keys;
        std::vector<uint64_t> keyFp64;
        std::vector<uint64_t> keyMk;

        {
            std::atomic<uint64_t> warmupFlushRecordsSeen{0};
            auto warmup = memtable::MemTable::create(makeOptions(config, writerThreads, flushMode, &warmupFlushRecordsSeen));
            generateKeyChunk(spec.keySize, 0, static_cast<size_t>(warmupOps), config.usePrehash, keys, keyFp64, keyMk);
            runPutParallel(
                *warmup,
                keys,
                value,
                config.usePrehash ? &keyFp64 : nullptr,
                config.usePrehash ? &keyMk : nullptr,
                writerThreads,
                nullptr
            );

            const uint64_t snapshot = warmup->lastSeq();
            RecordView out;
            for (int i = 0; i < warmupOps; ++i) {
                if (!warmup->get(asU8(keys[static_cast<size_t>(i)]), snapshot, &out)) {
                    std::fprintf(stderr, "WARMUP GET miss at i=%d\n", i);
                    std::exit(1);
                }
            }
        }

        std::atomic<uint64_t> flushRecordsSeen{0};
        auto memtable = memtable::MemTable::create(makeOptions(config, writerThreads, flushMode, &flushRecordsSeen));
        std::vector<uint32_t> putLatencyNs;
        std::vector<uint32_t> getLatencyNs;
        std::vector<uint32_t> scanLatencyNs;

        double putMs = 0.0;
        bool firstPutChunk = true;
        for (size_t begin = 0; begin < static_cast<size_t>(opsPerCase); begin += kKeyChunkSize) {
            const size_t count = std::min(kKeyChunkSize, static_cast<size_t>(opsPerCase) - begin);
            generateKeyChunk(spec.keySize, begin, count, config.usePrehash, keys, keyFp64, keyMk);
            std::vector<uint32_t>* latencyOut = firstPutChunk ? &putLatencyNs : nullptr;
            const auto putT0 = Clock::now();
            runPutParallel(
                *memtable,
                keys,
                value,
                config.usePrehash ? &keyFp64 : nullptr,
                config.usePrehash ? &keyMk : nullptr,
                writerThreads,
                latencyOut
            );
            putMs += std::chrono::duration<double, std::milli>(Clock::now() - putT0).count();
            firstPutChunk = false;
        }

        const uint64_t snapshot = memtable->lastSeq();
        double getMs = 0.0;
        bool firstGetChunk = true;
        for (size_t begin = 0; begin < static_cast<size_t>(opsPerCase); begin += kKeyChunkSize) {
            const size_t count = std::min(kKeyChunkSize, static_cast<size_t>(opsPerCase) - begin);
            generateKeyChunk(spec.keySize, begin, count, config.usePrehash, keys, keyFp64, keyMk);
            std::vector<uint32_t>* latencyOut = firstGetChunk ? &getLatencyNs : nullptr;
            const auto getT0 = Clock::now();
            runGetParallel(
                *memtable,
                keys,
                config.usePrehash ? &keyFp64 : nullptr,
                snapshot,
                writerThreads,
                latencyOut
            );
            getMs += std::chrono::duration<double, std::milli>(Clock::now() - getT0).count();
            firstGetChunk = false;
        }
        const auto scanT0 = Clock::now();
        const double scanOpsPerSec = runScanSingle(*memtable, snapshot, static_cast<size_t>(opsPerCase), &scanLatencyNs);
        const auto scanMs = std::chrono::duration<double, std::milli>(Clock::now() - scanT0).count();

        const double flushMs = 0.0;
        const double putFlushMs = putMs;
        const auto snapshotAfter = memtable->snapshot();

        return {
            .putOpsPerSec = static_cast<double>(opsPerCase) * 1000.0 / putMs,
            .getOpsPerSec = static_cast<double>(opsPerCase) * 1000.0 / getMs,
            .scanOpsPerSec = scanOpsPerSec,
            .putMs = putMs,
            .putFlushOpsPerSec = static_cast<double>(opsPerCase) * 1000.0 / putFlushMs,
            .putFlushMs = putFlushMs,
            .getMs = getMs,
            .scanMs = scanMs,
            .flushMs = flushMs,
            .approxBytes = snapshotAfter.approxBytes,
            .flushesCompleted = snapshotAfter.flushesCompleted,
            .flushRecordsSeen = flushRecordsSeen.load(std::memory_order_relaxed),
            .putLatency = buildPercentiles(putLatencyNs),
            .getLatency = buildPercentiles(getLatencyNs),
            .scanLatency = buildPercentiles(scanLatencyNs)
        };
    }
} // namespace

int main(int argc, char** argv) {
    akkaradb::test::installMsvcTestErrorHandlers();

    BenchConfig config;
    std::filesystem::path exePath = (argc > 0 && argv[0] != nullptr) ? std::filesystem::path{argv[0]} : std::filesystem::path{};

    if (!kDistributionMode) {
        for (int i = 1; i < argc; ++i) {
            const std::string arg = argv[i];
            if (arg == "--prehash") {
                config.usePrehash = true;
                continue;
            }
            if (arg == "--compare-flush-modes") {
                config.compareFlushModes = true;
                continue;
            }
            if (arg.rfind("--backend=", 0) == 0) {
                const std::string kind = arg.substr(10);
                if (kind == "skiplist") {
                    config.backend = BackendKind::SkipList;
                    continue;
                }
                if (kind == "bptree") {
                    config.backend = BackendKind::BPTree;
                    continue;
                }
                if (kind == "art") {
                    config.backend = BackendKind::ART;
                    continue;
                }
                std::fprintf(stderr, "Unknown backend: %s (use skiplist|bptree|art)\n", kind.c_str());
                return 2;
            }
            if (arg.rfind("--writers=", 0) == 0) {
                config.writerThreads = std::max(0, std::atoi(arg.substr(10).c_str()));
                continue;
            }
            if (arg.rfind("--shards=", 0) == 0) {
                config.requestedShards = static_cast<uint32_t>(std::max(0, std::atoi(arg.substr(9).c_str())));
                continue;
            }
            if (arg.rfind("--auto-cap=", 0) == 0) {
                config.autoShardCountCap = static_cast<uint32_t>(std::max(0, std::atoi(arg.substr(11).c_str())));
                continue;
            }
            if (arg.rfind("--repeats=", 0) == 0) {
                config.repeats = std::max(1, std::atoi(arg.substr(10).c_str()));
                continue;
            }
            if (arg.rfind("--output=", 0) == 0) {
                config.outputPath = arg.substr(9);
                continue;
            }
            config.opsPerCase = std::max(1000, std::atoi(arg.c_str()));
        }
    }

    if (config.outputPath.empty()) {
        const std::filesystem::path baseDir = exePath.has_parent_path() ? exePath.parent_path() : std::filesystem::current_path();
        config.outputPath = (baseDir / "memtable_throughput_results.csv").string();
    }

    const std::array<CaseSpec, 6> cases{{
        {8, 16},
        {16, 64},
        {16, 256},
        {32, 1024},
        {32, 4096},
        {64, 16384}
    }};

    const int effectiveWriters = resolveWriterThreads(config.writerThreads);
    const uint32_t shardCount = resolveShardCount(
        config.requestedShards,
        static_cast<size_t>(effectiveWriters),
        config.autoShardCountCap
    );

    const std::filesystem::path exeDir = exePath.has_parent_path() ? exePath.parent_path() : std::filesystem::current_path();
    const SystemProfile systemProfile = collectSystemProfile(config.outputPath, exeDir / "diskspd.exe");
    std::vector<FlushMode> flushModes;
    flushModes.push_back(FlushMode::NoFlush);
    if (config.compareFlushModes || kDistributionMode) {
        flushModes.push_back(FlushMode::DefaultThreshold);
    }
    std::vector<BackendKind> backendsToRun;
    if (kDistributionMode) {
        backendsToRun = {
            BackendKind::BPTree,
            BackendKind::ART,
            BackendKind::SkipList
        };
    } else {
        backendsToRun.push_back(config.backend);
    }

    std::printf("Sharded MemTable throughput benchmark\n");
    std::printf("opsPerCase = %d\n", config.opsPerCase);
    std::printf("repeats    = %d\n", config.repeats);
    if (kDistributionMode) {
        std::printf("distributionMode = ON (CLI benchmark options ignored)\n");
        std::printf("backends = bptree -> art -> skiplist\n");
    } else {
        std::printf("backend = %s\n", backendName(config.backend));
    }
    if (config.writerThreads == 0) {
        std::printf("writerThreads = auto (%d from hwThreads)\n", effectiveWriters);
    } else {
        std::printf("writerThreads = %d\n", config.writerThreads);
    }
    if (config.requestedShards == 0) {
        std::printf("resolvedShards = %u (auto, writers*4 locality heuristic, cap %u)\n", shardCount, config.autoShardCountCap);
    } else if (config.requestedShards == shardCount) {
        std::printf("resolvedShards = %u (explicit)\n", shardCount);
    } else {
        std::printf("resolvedShards = %u (explicit %u rounded to power-of-two)\n", shardCount, config.requestedShards);
    }
    std::printf("flushModes  = %s\n", (config.compareFlushModes || kDistributionMode) ? "disabled -> default_threshold" : "disabled");
    std::printf("warmupOps   = %d\n\n", std::min(config.opsPerCase, 50000));
    std::printf("prehashMode = %s\n\n", config.usePrehash ? "ON (fp64/mk precomputed)" : "OFF (hash inside put)");
    std::printf("outputPath  = %s\n\n", config.outputPath.c_str());
    std::printf("system.cpu = %s\n", systemProfile.cpuName.c_str());
    std::printf("system.ram = %s across %u module(s), mean speed %.0f MT/s\n",
                formatBytes(systemProfile.ramTotalBytes).c_str(),
                systemProfile.ramModuleCount,
                systemProfile.ramSpeedMeanMtps);
    std::printf("system.os  = %s (%s)\n", systemProfile.osName.c_str(), systemProfile.osArchitecture.c_str());
    std::printf("system.ssd = %s [%s] on drive %s\n\n",
                systemProfile.ssdModel.c_str(),
                systemProfile.ssdMediaType.c_str(),
                systemProfile.benchmarkDrive.c_str());
    if (systemProfile.disk.available) {
        std::printf("disk probe = %s, file=%s, size=%s, runs=%u\n\n",
                    "OK",
                    systemProfile.disk.tempFilePath.c_str(),
                    formatBytes(systemProfile.disk.fileSizeBytes).c_str(),
                    systemProfile.disk.runs);
    } else if (!systemProfile.disk.error.empty()) {
        std::printf("disk probe = FAILED (%s)\n\n", systemProfile.disk.error.c_str());
    }

    std::vector<BackendReport> backendReports;
    backendReports.reserve(backendsToRun.size());
    for (const BackendKind backend : backendsToRun) {
        BenchConfig backendConfig = config;
        backendConfig.backend = backend;
        std::printf("=== backend: %s ===\n", backendName(backend));

        BackendReport backendReport{};
        backendReport.backend = backend;
        backendReport.modeReports.reserve(flushModes.size());
        for (const FlushMode flushMode : flushModes) {
            std::printf("=== flush_mode: %s ===\n", flushModeName(flushMode));
            std::printf("%-18s %-10s %-12s %-8s %-8s %-14s %-14s %-14s %-14s %-8s %-8s %-8s\n",
                        "flush_mode", "key", "value", "shards", "writers", "put(ops/s)", "get(ops/s)", "scan(ops/s)", "memBytes", "putSmp", "getSmp", "scanSmp");
            std::printf("%-18s %-10s %-12s %-8s %-8s %-14s %-14s %-14s %-14s %-8s %-8s %-8s\n",
                        "", "", "", "", "", "", "", "", "", "P50/P90/P99/P999(us)", "P50/P90/P99/P999(us)", "P50/P90/P99/P999(us)");
            std::printf("----------------------------------------------------------------------------------------------------------------------\n");

            FlushModeReport modeReport{};
            modeReport.flushMode = flushMode;
            modeReport.reports.reserve(cases.size());

            for (const auto& spec : cases) {
                std::vector<ThroughputResult> runs;
                runs.reserve(static_cast<size_t>(config.repeats));
                for (int repeat = 0; repeat < config.repeats; ++repeat) {
                    runs.push_back(runCase(spec, config.opsPerCase, effectiveWriters, backendConfig, flushMode));
                }
                const ThroughputResult result = averageResults(runs);
                modeReport.reports.push_back(CaseReport{
                    .spec = spec,
                    .averaged = result,
                    .stats = computeResultStats(runs)
                });
                std::printf("%-18s %-10d %-12d %-8u %-8d %-14.0f %-14.0f %-14.0f %-14llu %-8u %-8u %-8u\n",
                            flushModeName(flushMode),
                            spec.keySize,
                            spec.valueSize,
                            shardCount,
                            effectiveWriters,
                            result.putOpsPerSec,
                            result.getOpsPerSec,
                            result.scanOpsPerSec,
                            static_cast<unsigned long long>(result.approxBytes),
                            result.putLatency.sampleCount,
                            result.getLatency.sampleCount,
                            result.scanLatency.sampleCount);
                std::printf("%-18s %-10s %-12s %-8s %-8s %-14s %-14s %-14s %-14s %4.2f/%4.2f/%4.2f/%4.2f %4.2f/%4.2f/%4.2f/%4.2f %4.2f/%4.2f/%4.2f/%4.2f\n",
                            "", "", "", "", "", "", "", "", "",
                            result.putLatency.p50Us,
                            result.putLatency.p90Us,
                            result.putLatency.p99Us,
                            result.putLatency.p999Us,
                            result.getLatency.p50Us,
                            result.getLatency.p90Us,
                            result.getLatency.p99Us,
                            result.getLatency.p999Us,
                            result.scanLatency.p50Us,
                            result.scanLatency.p90Us,
                            result.scanLatency.p99Us,
                            result.scanLatency.p999Us);
                std::printf("  timings(ms): put=%8.2f get=%8.2f scan=%8.2f   payload(MiB/s): put=%8.2f get=%8.2f scan=%8.2f   flushes=%llu flushRecords=%llu\n",
                            result.putMs,
                            result.getMs,
                            result.scanMs,
                            payloadMibPerSec(static_cast<size_t>(spec.keySize + spec.valueSize), config.opsPerCase, result.putMs),
                            payloadMibPerSec(static_cast<size_t>(spec.keySize + spec.valueSize), config.opsPerCase, result.getMs),
                            payloadMibPerSec(static_cast<size_t>(spec.keySize + spec.valueSize), config.opsPerCase, result.scanMs),
                            static_cast<unsigned long long>(result.flushesCompleted),
                            static_cast<unsigned long long>(result.flushRecordsSeen));
                std::printf("----------------------------------------------------------------------------------------------------------------------\n");
                std::fflush(stdout);
            }
            backendReport.modeReports.push_back(std::move(modeReport));
        }
        backendReports.push_back(std::move(backendReport));
    }

    writeReportsCsv(config.outputPath, config, effectiveWriters, shardCount, systemProfile, backendReports);

    return 0;
}

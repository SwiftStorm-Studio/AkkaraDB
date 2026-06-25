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

// benchmarks/throughput/sstableThroughputBenchmark.cpp
#include "TestErrorHandlers.hpp"

/*
 * SSTable throughput benchmark (size sweep).
 *
 * Measures:
 *  - SSTWriter write throughput
 *  - SSTReader point-read throughput
 *  - SSTReader scan throughput
 *
 * Usage:
 *   akkaradbSstableThroughputBenchmark [opsPerCase]
 *       [--readers=N|--writers=N] [--codec=none|zstd]
 *       [--block-size=N] [--cache-bytes=N|NKiB|NMiB|NGiB]
 *       [--max-case-bytes=N|NKiB|NMiB|NGiB] [--fixed-ops|--same-ops]
 *
 * Default:
 *   opsPerCase = 500000
 *   readers      = 16
 *   codec        = zstd
 *   max payload   = 512 MiB per case unless --fixed-ops is set
 */

#include "akk/core/record/KeyFingerprint.hpp"
#include "akk/core/record/SSTHdr32.hpp"
#include "akk/engine/sstable/SSTReader.hpp"
#include "akk/engine/sstable/SSTWriter.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <format>
#include <limits>
#include <new>
#include <numeric>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#include <malloc.h>
#endif

using Clock = std::chrono::steady_clock;
using namespace akkaradb::core;
namespace sst = akkaradb::engine::sst;

namespace {
    namespace fs = std::filesystem;

    constexpr uint32_t kLatencySampleMask = 0x3F; // sample 1 / 64 ops to reduce benchmark perturbation
    constexpr size_t kScanSampleWindow = 32; // amortize clock resolution/overhead for iterator scan()
    constexpr uint64_t kDiskBenchmarkFileSizeBytes = 256ULL * 1024ULL * 1024ULL;
    constexpr uint32_t kDiskBenchmarkRuns = 4;

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

    struct BenchConfig {
        int opsPerCase = 500000;
        int repeats = 10;
        int readerThreads = 16;
        uint32_t blockSize = sst::SST_DEFAULT_BLOCK_SIZE;
        uint64_t blockCacheBytes = 64ULL * 1024ULL * 1024ULL;
        uint64_t maxCasePayloadBytes = 512ULL * 1024ULL * 1024ULL;
        bool fixedOps = false;
        sst::SSTWriter::Codec codec = sst::SSTWriter::Codec::ZSTD;
        std::string outputPath;
    };

    struct ThroughputResult {
        int ops = 0;
        double writeOpsPerSec = 0.0;
        double openMs = 0.0;
        double getOpsPerSec = 0.0;
        double scanOpsPerSec = 0.0;
        double writeMs = 0.0;
        double getMs = 0.0;
        double scanMs = 0.0;
        uint64_t fileBytes = 0;
        uint64_t scanRecords = 0;
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
        NumericStats writeOpsPerSec;
        NumericStats openMs;
        NumericStats getOpsPerSec;
        NumericStats scanOpsPerSec;
        NumericStats writeMs;
        NumericStats getMs;
        NumericStats scanMs;
        NumericStats fileBytes;
        NumericStats scanRecords;
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

    struct DiskMetricStats {
        double throughputMeanMiBPerSec = 0.0;
        double throughputMedianMiBPerSec = 0.0;
        double iopsMean = 0.0;
        double iopsMedian = 0.0;
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

    [[nodiscard]] static std::span<const uint8_t> asU8(const std::string& s) {
        return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
    }

    [[nodiscard]] static std::string lowerAscii(std::string s) {
        for (char& ch : s) {
            if (ch >= 'A' && ch <= 'Z') {
                ch = static_cast<char>(ch - 'A' + 'a');
            }
        }
        return s;
    }

    [[nodiscard]] static bool parseU64WithSuffix(const std::string& text, uint64_t* out) {
        if (text.empty() || out == nullptr) {
            return false;
        }

        char* end = nullptr;
        const unsigned long long base = std::strtoull(text.c_str(), &end, 10);
        if (end == text.c_str()) {
            return false;
        }

        const std::string suffix = lowerAscii(std::string{end});
        uint64_t multiplier = 1;
        if (suffix.empty() || suffix == "b") {
            multiplier = 1;
        } else if (suffix == "k" || suffix == "kb" || suffix == "kib") {
            multiplier = 1024ULL;
        } else if (suffix == "m" || suffix == "mb" || suffix == "mib") {
            multiplier = 1024ULL * 1024ULL;
        } else if (suffix == "g" || suffix == "gb" || suffix == "gib") {
            multiplier = 1024ULL * 1024ULL * 1024ULL;
        } else {
            return false;
        }

        if (base > std::numeric_limits<uint64_t>::max() / multiplier) {
            return false;
        }
        *out = static_cast<uint64_t>(base) * multiplier;
        return true;
    }

    [[nodiscard]] static double bytesToMib(uint64_t bytes) {
        return static_cast<double>(bytes) / (1024.0 * 1024.0);
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

    [[nodiscard]] static double mibPerSec(uint64_t bytes, double ms) {
        if (ms <= 0.0) {
            return 0.0;
        }
        return bytesToMib(bytes) * 1000.0 / ms;
    }

    [[nodiscard]] static double payloadMibPerSec(size_t bytesPerOp, int ops, double ms) {
        if (ms <= 0.0) {
            return 0.0;
        }
        const double totalMib = static_cast<double>(bytesPerOp) * static_cast<double>(ops) / (1024.0 * 1024.0);
        return totalMib * 1000.0 / ms;
    }

    [[nodiscard]] static double quantileFromSorted(const std::vector<uint32_t>& sortedNs, double q) {
        if (sortedNs.empty()) {
            return 0.0;
        }
        const double qClamped = std::clamp(q, 0.0, 1.0);
        const size_t idx = static_cast<size_t>(qClamped * static_cast<double>(sortedNs.size() - 1));
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

    [[nodiscard]] static NumericStats computeNumericStats(std::vector<double> values) {
        NumericStats out{};
        if (values.empty()) {
            return out;
        }
        const double sum = std::accumulate(values.begin(), values.end(), 0.0);
        out.mean = sum / static_cast<double>(values.size());
        std::sort(values.begin(), values.end());
        const size_t mid = values.size() / 2;
        out.median = (values.size() & 1U) == 0U ? (values[mid - 1] + values[mid]) * 0.5 : values[mid];
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

    [[nodiscard]] static ThroughputResult averageResults(const std::vector<ThroughputResult>& runs) {
        ThroughputResult out{};
        if (runs.empty()) {
            return out;
        }
        for (const auto& run : runs) {
            out.ops += run.ops;
            out.writeOpsPerSec += run.writeOpsPerSec;
            out.openMs += run.openMs;
            out.getOpsPerSec += run.getOpsPerSec;
            out.scanOpsPerSec += run.scanOpsPerSec;
            out.writeMs += run.writeMs;
            out.getMs += run.getMs;
            out.scanMs += run.scanMs;
            out.fileBytes += run.fileBytes;
            out.scanRecords += run.scanRecords;
            accumulateLatency(out.getLatency, run.getLatency);
            accumulateLatency(out.scanLatency, run.scanLatency);
        }
        const double divisor = static_cast<double>(runs.size());
        out.ops = static_cast<int>(std::llround(static_cast<double>(out.ops) / divisor));
        out.writeOpsPerSec /= divisor;
        out.openMs /= divisor;
        out.getOpsPerSec /= divisor;
        out.scanOpsPerSec /= divisor;
        out.writeMs /= divisor;
        out.getMs /= divisor;
        out.scanMs /= divisor;
        out.fileBytes = static_cast<uint64_t>(std::llround(static_cast<double>(out.fileBytes) / divisor));
        out.scanRecords = static_cast<uint64_t>(std::llround(static_cast<double>(out.scanRecords) / divisor));
        divideLatency(out.getLatency, divisor);
        divideLatency(out.scanLatency, divisor);
        return out;
    }

    [[nodiscard]] static ResultStats computeResultStats(const std::vector<ThroughputResult>& runs) {
        ResultStats stats{};
        if (runs.empty()) {
            return stats;
        }
        std::vector<double> writeOps, openMs, getOps, scanOps, writeMs, getMs, scanMs, fileBytes, scanRecords;
        std::vector<double> getP50, getP90, getP99, getP999, scanP50, scanP90, scanP99, scanP999;
        writeOps.reserve(runs.size());
        openMs.reserve(runs.size());
        getOps.reserve(runs.size());
        scanOps.reserve(runs.size());
        writeMs.reserve(runs.size());
        getMs.reserve(runs.size());
        scanMs.reserve(runs.size());
        fileBytes.reserve(runs.size());
        scanRecords.reserve(runs.size());
        getP50.reserve(runs.size());
        getP90.reserve(runs.size());
        getP99.reserve(runs.size());
        getP999.reserve(runs.size());
        scanP50.reserve(runs.size());
        scanP90.reserve(runs.size());
        scanP99.reserve(runs.size());
        scanP999.reserve(runs.size());
        for (const auto& run : runs) {
            writeOps.push_back(run.writeOpsPerSec);
            openMs.push_back(run.openMs);
            getOps.push_back(run.getOpsPerSec);
            scanOps.push_back(run.scanOpsPerSec);
            writeMs.push_back(run.writeMs);
            getMs.push_back(run.getMs);
            scanMs.push_back(run.scanMs);
            fileBytes.push_back(static_cast<double>(run.fileBytes));
            scanRecords.push_back(static_cast<double>(run.scanRecords));
            getP50.push_back(run.getLatency.p50Us);
            getP90.push_back(run.getLatency.p90Us);
            getP99.push_back(run.getLatency.p99Us);
            getP999.push_back(run.getLatency.p999Us);
            scanP50.push_back(run.scanLatency.p50Us);
            scanP90.push_back(run.scanLatency.p90Us);
            scanP99.push_back(run.scanLatency.p99Us);
            scanP999.push_back(run.scanLatency.p999Us);
        }
        stats.writeOpsPerSec = computeNumericStats(std::move(writeOps));
        stats.openMs = computeNumericStats(std::move(openMs));
        stats.getOpsPerSec = computeNumericStats(std::move(getOps));
        stats.scanOpsPerSec = computeNumericStats(std::move(scanOps));
        stats.writeMs = computeNumericStats(std::move(writeMs));
        stats.getMs = computeNumericStats(std::move(getMs));
        stats.scanMs = computeNumericStats(std::move(scanMs));
        stats.fileBytes = computeNumericStats(std::move(fileBytes));
        stats.scanRecords = computeNumericStats(std::move(scanRecords));
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

    [[nodiscard]] static std::string makeFixedBytes(int size, uint64_t seed) {
        std::string out;
        out.resize(static_cast<size_t>(size));
        uint64_t x = seed ^ 0x9e3779b97f4a7c15ULL;
        for (int i = 0; i < size; ++i) {
            x ^= (x << 13);
            x ^= (x >> 7);
            x ^= (x << 17);
            out[static_cast<size_t>(i)] = static_cast<char>('a' + (x % 26));
        }
        return out;
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

    [[nodiscard]] static std::optional<uint64_t> parseU64Plain(const std::string& text) {
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

    [[nodiscard]] static std::string escapePowerShellSingleQuoted(std::string value) {
        size_t pos = 0;
        while ((pos = value.find('\'', pos)) != std::string::npos) {
            value.replace(pos, 1, "''");
            pos += 2;
        }
        return value;
    }

    [[nodiscard]] static std::string benchmarkDriveFromPath(const fs::path& path) {
        const fs::path absolute = fs::absolute(path);
        const std::string root = absolute.root_name().string();
        if (root.size() >= 2 && root[1] == ':') {
            return std::string{static_cast<char>(std::toupper(static_cast<unsigned char>(root[0])))};
        }
        return "C";
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

#ifdef _WIN32
    struct AlignedBuffer {
        void* ptr = nullptr;
        size_t size = 0;
        explicit AlignedBuffer(size_t n) : ptr(_aligned_malloc(n, 4096)), size(n) {
            if (ptr == nullptr) {
                throw std::bad_alloc{};
            }
        }
        ~AlignedBuffer() {
            if (ptr != nullptr) {
                _aligned_free(ptr);
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
    };

    struct IoSlot {
        OVERLAPPED overlapped{};
        HANDLE eventHandle = nullptr;
        AlignedBuffer buffer;
        explicit IoSlot(size_t blockSize) : eventHandle(CreateEventW(nullptr, TRUE, FALSE, nullptr)), buffer(blockSize) {
            if (eventHandle == nullptr) {
                throw std::runtime_error("CreateEventW failed for disk benchmark");
            }
            overlapped.hEvent = eventHandle;
            std::memset(buffer.ptr, 0xA5, blockSize);
        }
        ~IoSlot() {
            if (eventHandle != nullptr) {
                CloseHandle(eventHandle);
            }
        }
        IoSlot(const IoSlot&) = delete;
        IoSlot& operator=(const IoSlot&) = delete;
        IoSlot(IoSlot&& other) noexcept
            : overlapped(other.overlapped), eventHandle(other.eventHandle), buffer(std::move(other.buffer)) {
            other.eventHandle = nullptr;
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
                other.eventHandle = nullptr;
                std::memset(&other.overlapped, 0, sizeof(other.overlapped));
            }
            return *this;
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

    static void ensureBenchmarkFileSize(const fs::path& path, uint64_t fileSizeBytes) {
        HANDLE handle = CreateFileW(path.wstring().c_str(), GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
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

    struct DiskRunResult {
        double throughputMiBPerSec = 0.0;
        double iops = 0.0;
    };

    [[nodiscard]] static DiskRunResult runWindowsDiskIoCase(const fs::path& path, bool writeMode, bool randomAccess, uint32_t queueDepth, size_t blockSize, uint64_t fileSizeBytes, uint64_t seed) {
        const DWORD desiredAccess = writeMode ? (GENERIC_READ | GENERIC_WRITE) : GENERIC_READ;
        DWORD flags = FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OVERLAPPED | FILE_FLAG_NO_BUFFERING;
        flags |= randomAccess ? FILE_FLAG_RANDOM_ACCESS : FILE_FLAG_SEQUENTIAL_SCAN;
        if (writeMode) {
            flags |= FILE_FLAG_WRITE_THROUGH;
        }
        HANDLE handle = CreateFileW(path.wstring().c_str(), desiredAccess, FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr, OPEN_EXISTING, flags, nullptr);
        if (handle == INVALID_HANDLE_VALUE) {
            throw std::runtime_error("CreateFileW failed while opening disk benchmark file");
        }
        const uint64_t totalOps = fileSizeBytes / static_cast<uint64_t>(blockSize);
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
            return (xorshift64(state) % totalOps) * static_cast<uint64_t>(blockSize);
        };
        const auto submitOp = [&](IoSlot& slot, uint64_t opIndex) {
            ResetEvent(slot.eventHandle);
            std::memset(&slot.overlapped, 0, sizeof(slot.overlapped));
            slot.overlapped.hEvent = slot.eventHandle;
            assignOverlappedOffset(slot.overlapped, nextOffset(opIndex));
            DWORD transferred = 0;
            const BOOL ok = writeMode
                ? WriteFile(handle, slot.buffer.ptr, static_cast<DWORD>(blockSize), &transferred, &slot.overlapped)
                : ReadFile(handle, slot.buffer.ptr, static_cast<DWORD>(blockSize), &transferred, &slot.overlapped);
            if (!ok && GetLastError() != ERROR_IO_PENDING) {
                throw std::runtime_error("Disk benchmark I/O submit failed");
            }
        };
        for (uint32_t i = 0; i < static_cast<uint32_t>(std::min<uint64_t>(queueDepth, totalOps)); ++i) {
            submitOp(slots[static_cast<size_t>(i)], submitted++);
        }
        const auto t0 = Clock::now();
        while (completed < totalOps) {
            const DWORD wait = WaitForMultipleObjects(static_cast<DWORD>(events.size()), events.data(), FALSE, INFINITE);
            if (wait < WAIT_OBJECT_0 || wait >= WAIT_OBJECT_0 + events.size()) {
                CloseHandle(handle);
                throw std::runtime_error("WaitForMultipleObjects failed during disk benchmark");
            }
            IoSlot& slot = slots[static_cast<size_t>(wait - WAIT_OBJECT_0)];
            DWORD transferred = 0;
            if (!GetOverlappedResult(handle, &slot.overlapped, &transferred, FALSE) || transferred != blockSize) {
                CloseHandle(handle);
                throw std::runtime_error("Disk benchmark I/O completion failed");
            }
            ++completed;
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
        return {
            .throughputMiBPerSec = seconds > 0.0 ? (static_cast<double>(fileSizeBytes) / (1024.0 * 1024.0)) / seconds : 0.0,
            .iops = seconds > 0.0 ? static_cast<double>(totalOps) / seconds : 0.0
        };
    }
#endif

    [[nodiscard]] static SystemProfile collectSystemProfile(const fs::path& outputPath) {
        SystemProfile profile{};
        profile.benchmarkDrive = benchmarkDriveFromPath(outputPath);
#ifdef _WIN32
        const std::string driveLetter = escapePowerShellSingleQuoted(profile.benchmarkDrive);
        const std::string ps = std::format(
            "powershell -NoProfile -ExecutionPolicy Bypass -Command "
            "\"$ErrorActionPreference='SilentlyContinue'; [Console]::OutputEncoding=[System.Text.Encoding]::UTF8; "
            "$drive='{}'; $cpu=Get-CimInstance Win32_Processor | Select-Object -First 1 Name,NumberOfCores,NumberOfLogicalProcessors; "
            "$os=Get-CimInstance Win32_OperatingSystem | Select-Object -First 1 Caption,OSArchitecture; "
            "$mem=@(Get-CimInstance Win32_PhysicalMemory | Where-Object {{$_.Capacity -gt 0}}); "
            "$part=Get-Partition -DriveLetter $drive -ErrorAction SilentlyContinue | Select-Object -First 1; $disk=$null; "
            "if($part) {{$disk=Get-Disk -Number $part.DiskNumber -ErrorAction SilentlyContinue | Select-Object -First 1 FriendlyName,Model,MediaType;}} "
            "if(-not $disk) {{$disk=Get-CimInstance Win32_DiskDrive | Select-Object -First 1 Model,MediaType;}} "
            "$ramTotal=($mem | Measure-Object -Property Capacity -Sum).Sum; $ramCount=@($mem).Count; "
            "$ramSpeeds=@($mem | Where-Object {{$_.Speed -gt 0}} | ForEach-Object {{$_.Speed}}); $ramSpeedMean=0; "
            "if($ramSpeeds.Count -gt 0) {{$ramSpeedMean=[Math]::Round((($ramSpeeds | Measure-Object -Average).Average),2);}} "
            "$ramSpeedList=($ramSpeeds -join ';'); "
            "Write-Output ('cpu_name=' + $cpu.Name); Write-Output ('cpu_cores=' + $cpu.NumberOfCores); Write-Output ('cpu_logical=' + $cpu.NumberOfLogicalProcessors); "
            "Write-Output ('os_name=' + $os.Caption); Write-Output ('os_arch=' + $os.OSArchitecture); "
            "Write-Output ('ram_total=' + $ramTotal); Write-Output ('ram_modules=' + $ramCount); Write-Output ('ram_speed_mean=' + $ramSpeedMean); Write-Output ('ram_speed_list=' + $ramSpeedList); "
            "if($disk.Model) {{ Write-Output ('disk_model=' + $disk.Model); }} elseif($disk.FriendlyName) {{ Write-Output ('disk_model=' + $disk.FriendlyName); }} else {{ Write-Output 'disk_model='; }} "
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
                if (key == "cpu_name") profile.cpuName = value.empty() ? profile.cpuName : value;
                else if (key == "cpu_cores") profile.cpuPhysicalCores = static_cast<uint32_t>(parseU64Plain(value).value_or(0));
                else if (key == "cpu_logical") profile.cpuLogicalCores = static_cast<uint32_t>(parseU64Plain(value).value_or(0));
                else if (key == "os_name") profile.osName = value.empty() ? profile.osName : value;
                else if (key == "os_arch") profile.osArchitecture = value.empty() ? profile.osArchitecture : value;
                else if (key == "ram_total") profile.ramTotalBytes = parseU64Plain(value).value_or(0);
                else if (key == "ram_modules") profile.ramModuleCount = static_cast<uint32_t>(parseU64Plain(value).value_or(0));
                else if (key == "ram_speed_mean") profile.ramSpeedMeanMtps = parseDouble(value).value_or(0.0);
                else if (key == "ram_speed_list") profile.ramSpeedListMtps = value;
                else if (key == "disk_model") profile.ssdModel = value.empty() ? profile.ssdModel : value;
                else if (key == "disk_media") profile.ssdMediaType = value.empty() ? profile.ssdMediaType : value;
            }
            if (end == std::string::npos) {
                break;
            }
            start = end + 1;
        }
        try {
            const fs::path tempFile = fs::absolute(outputPath).parent_path() / "sstable_disk_probe.tmp";
            profile.disk.tempFilePath = tempFile.string();
            profile.disk.fileSizeBytes = kDiskBenchmarkFileSizeBytes;
            profile.disk.runs = kDiskBenchmarkRuns;
            ensureBenchmarkFileSize(tempFile, kDiskBenchmarkFileSizeBytes);
            auto runMetric = [&](bool writeMode, bool randomAccess, uint32_t queueDepth, size_t blockSize, uint64_t seed) {
                std::vector<double> throughputs;
                std::vector<double> iops;
                throughputs.reserve(kDiskBenchmarkRuns);
                iops.reserve(kDiskBenchmarkRuns);
                for (uint32_t run = 0; run < kDiskBenchmarkRuns; ++run) {
                    const auto result = runWindowsDiskIoCase(tempFile, writeMode, randomAccess, queueDepth, blockSize, kDiskBenchmarkFileSizeBytes, seed + run);
                    throughputs.push_back(result.throughputMiBPerSec);
                    iops.push_back(result.iops);
                }
                return computeDiskMetricStats(throughputs, iops);
            };
            profile.disk.seq1mQ8T1Write = runMetric(true, false, 8, 1024 * 1024, 0x1000);
            profile.disk.seq1mQ1T1Write = runMetric(true, false, 1, 1024 * 1024, 0x2000);
            profile.disk.rnd4kQ32T1Write = runMetric(true, true, 32, 4 * 1024, 0x3000);
            profile.disk.rnd4kQ1T1Write = runMetric(true, true, 1, 4 * 1024, 0x4000);
            profile.disk.seq1mQ8T1Read = runMetric(false, false, 8, 1024 * 1024, 0x5000);
            profile.disk.seq1mQ1T1Read = runMetric(false, false, 1, 1024 * 1024, 0x6000);
            profile.disk.rnd4kQ32T1Read = runMetric(false, true, 32, 4 * 1024, 0x7000);
            profile.disk.rnd4kQ1T1Read = runMetric(false, true, 1, 4 * 1024, 0x8000);
            profile.disk.available = true;
            std::error_code ignoreEc;
            fs::remove(tempFile, ignoreEc);
        } catch (const std::exception& ex) {
            profile.disk.error = ex.what();
        }
#endif
        return profile;
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
        const SystemProfile& profile,
        int effectiveReaders,
        const char* codec,
        const std::vector<CaseReport>& reports
    ) {
        if (path.empty()) {
            return;
        }
        std::ofstream out(path, std::ios::out | std::ios::trunc);
        if (!out) {
            throw std::runtime_error("Failed to open benchmark output file: " + path);
        }
        out << "codec,ops_per_case,repeats,key_size,value_size,case_ops,readers,block_size,block_cache_bytes,max_case_payload_bytes,fixed_ops,"
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
               "write_ops_s,open_ms,get_ops_s,scan_ops_s,write_ms,get_ms,scan_ms,file_bytes,scan_records,get_smp,scan_smp,get_p50_us,get_p90_us,get_p99_us,get_p999_us,scan_p50_us,scan_p90_us,scan_p99_us,scan_p999_us,"
               "write_ops_s_mean,write_ops_s_median,write_ops_s_stddev,write_ops_s_cov,"
               "open_ms_mean,open_ms_median,open_ms_stddev,open_ms_cov,"
               "get_ops_s_mean,get_ops_s_median,get_ops_s_stddev,get_ops_s_cov,"
               "scan_ops_s_mean,scan_ops_s_median,scan_ops_s_stddev,scan_ops_s_cov,"
               "write_ms_mean,write_ms_median,write_ms_stddev,write_ms_cov,"
               "get_ms_mean,get_ms_median,get_ms_stddev,get_ms_cov,"
               "scan_ms_mean,scan_ms_median,scan_ms_stddev,scan_ms_cov,"
               "file_bytes_mean,file_bytes_median,file_bytes_stddev,file_bytes_cov,"
               "scan_records_mean,scan_records_median,scan_records_stddev,scan_records_cov,"
               "get_p50_us_mean,get_p50_us_median,get_p50_us_stddev,get_p50_us_cov,"
               "get_p90_us_mean,get_p90_us_median,get_p90_us_stddev,get_p90_us_cov,"
               "get_p99_us_mean,get_p99_us_median,get_p99_us_stddev,get_p99_us_cov,"
               "get_p999_us_mean,get_p999_us_median,get_p999_us_stddev,get_p999_us_cov,"
               "scan_p50_us_mean,scan_p50_us_median,scan_p50_us_stddev,scan_p50_us_cov,"
               "scan_p90_us_mean,scan_p90_us_median,scan_p90_us_stddev,scan_p90_us_cov,"
               "scan_p99_us_mean,scan_p99_us_median,scan_p99_us_stddev,scan_p99_us_cov,"
               "scan_p999_us_mean,scan_p999_us_median,scan_p999_us_stddev,scan_p999_us_cov\n";
        for (const auto& report : reports) {
            writeCsvField(out, codec);
            out << ',' << config.opsPerCase
                << ',' << config.repeats
                << ',' << report.spec.keySize
                << ',' << report.spec.valueSize
                << ',' << report.averaged.ops
                << ',' << effectiveReaders
                << ',' << config.blockSize
                << ',' << config.blockCacheBytes
                << ',' << config.maxCasePayloadBytes
                << ',' << (config.fixedOps ? 1 : 0)
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
            out << ',' << (profile.disk.available ? 1 : 0) << ',';
            writeCsvField(out, profile.disk.error);
            out << ',' << profile.disk.fileSizeBytes << ',' << profile.disk.runs << ',';
            writeCsvField(out, profile.disk.tempFilePath);
            writeDiskMetricColumns(out, profile.disk.seq1mQ8T1Read);
            writeDiskMetricColumns(out, profile.disk.seq1mQ8T1Write);
            writeDiskMetricColumns(out, profile.disk.seq1mQ1T1Read);
            writeDiskMetricColumns(out, profile.disk.seq1mQ1T1Write);
            writeDiskMetricColumns(out, profile.disk.rnd4kQ32T1Read);
            writeDiskMetricColumns(out, profile.disk.rnd4kQ32T1Write);
            writeDiskMetricColumns(out, profile.disk.rnd4kQ1T1Read);
            writeDiskMetricColumns(out, profile.disk.rnd4kQ1T1Write);
            out << ',' << report.averaged.writeOpsPerSec
                << ',' << report.averaged.openMs
                << ',' << report.averaged.getOpsPerSec
                << ',' << report.averaged.scanOpsPerSec
                << ',' << report.averaged.writeMs
                << ',' << report.averaged.getMs
                << ',' << report.averaged.scanMs
                << ',' << report.averaged.fileBytes
                << ',' << report.averaged.scanRecords
                << ',' << report.averaged.getLatency.sampleCount
                << ',' << report.averaged.scanLatency.sampleCount
                << ',' << report.averaged.getLatency.p50Us
                << ',' << report.averaged.getLatency.p90Us
                << ',' << report.averaged.getLatency.p99Us
                << ',' << report.averaged.getLatency.p999Us
                << ',' << report.averaged.scanLatency.p50Us
                << ',' << report.averaged.scanLatency.p90Us
                << ',' << report.averaged.scanLatency.p99Us
                << ',' << report.averaged.scanLatency.p999Us;
            writeStatColumns(out, report.stats.writeOpsPerSec);
            writeStatColumns(out, report.stats.openMs);
            writeStatColumns(out, report.stats.getOpsPerSec);
            writeStatColumns(out, report.stats.scanOpsPerSec);
            writeStatColumns(out, report.stats.writeMs);
            writeStatColumns(out, report.stats.getMs);
            writeStatColumns(out, report.stats.scanMs);
            writeStatColumns(out, report.stats.fileBytes);
            writeStatColumns(out, report.stats.scanRecords);
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

    [[nodiscard]] static fs::path makeTempDir(const std::string& suffix) {
        auto dir = fs::temp_directory_path() / ("akkaradbSstableBench_" + suffix);
        std::error_code ec;
        fs::remove_all(dir, ec);
        fs::create_directories(dir, ec);
        if (ec) {
            std::fprintf(stderr, "failed to create temp dir: %s\n", dir.string().c_str());
            std::exit(6);
        }
        return dir;
    }

    static void removeTempDir(const fs::path& dir) {
        std::error_code ec;
        fs::remove_all(dir, ec);
    }

    [[nodiscard]] static int resolveReaderThreads(int requested) {
        if (requested > 0) {
            return requested;
        }
        const unsigned hw = std::thread::hardware_concurrency();
        return static_cast<int>(hw == 0 ? 1u : hw);
    }

    struct Records {
        std::vector<std::string> keys;
        std::string value;
        std::vector<RecordView> views;
    };

    [[nodiscard]] static Records makeRecords(CaseSpec spec, int count) {
        Records records;
        records.keys.reserve(static_cast<size_t>(count));
        records.views.reserve(static_cast<size_t>(count));
        records.value = makeFixedBytes(spec.valueSize, 0xA11CEULL);

        for (int i = 0; i < count; ++i) {
            std::string key = makeFixedBytes(spec.keySize, static_cast<uint64_t>(i) + 1);
            if (spec.keySize >= 10) {
                const auto tail = std::format("{:010d}", i);
                std::memcpy(key.data() + (spec.keySize - 10), tail.data(), 10);
            }
            records.keys.emplace_back(std::move(key));
        }

        std::sort(records.keys.begin(), records.keys.end());

        for (int i = 0; i < count; ++i) {
            const auto& key = records.keys[static_cast<size_t>(i)];
            const auto keySpan = asU8(key);
            const uint64_t fp = computeKeyFp64(keySpan.data(), keySpan.size());
            const uint64_t mk = buildMiniKey(keySpan.data(), keySpan.size());
            records.views.emplace_back(
                keySpan.data(),
                static_cast<uint16_t>(keySpan.size()),
                reinterpret_cast<const uint8_t*>(records.value.data()),
                static_cast<uint16_t>(records.value.size()),
                static_cast<uint64_t>(i + 1),
                SSTHdr32::FLAG_NORMAL,
                fp,
                mk
            );
        }
        return records;
    }

    static void runGetParallel(
        const sst::SSTReader& reader,
        const std::vector<std::string>& keys,
        int readerThreads,
        std::vector<uint32_t>* latencySamplesNs
    ) {
        if (latencySamplesNs) {
            latencySamplesNs->clear();
        }

        if (readerThreads <= 1) {
            std::vector<uint8_t> out;
            if (latencySamplesNs) {
                latencySamplesNs->reserve((keys.size() + kLatencySampleMask) / (kLatencySampleMask + 1));
            }
            for (size_t i = 0; i < keys.size(); ++i) {
                const bool doSample = ((static_cast<uint32_t>(i) & kLatencySampleMask) == 0);
                const auto t0 = doSample ? Clock::now() : Clock::time_point{};
                const auto found = reader.getInto(asU8(keys[i]), out);
                if (!found.has_value() || !*found) {
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
                std::vector<uint8_t> out;
                auto& samples = localSamples[static_cast<size_t>(tid)];
                for (size_t i = static_cast<size_t>(tid); i < keys.size(); i += static_cast<size_t>(readerThreads)) {
                    const bool doSample = ((static_cast<uint32_t>(i) & kLatencySampleMask) == 0);
                    const auto t0 = doSample ? Clock::now() : Clock::time_point{};
                    const auto found = reader.getInto(asU8(keys[i]), out);
                    if (!found.has_value() || !*found) {
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

    [[nodiscard]] static uint64_t runScan(
        const sst::SSTReader& reader,
        size_t expectedRecords,
        std::vector<uint32_t>* latencySamplesNs
    ) {
        if (latencySamplesNs) {
            latencySamplesNs->clear();
            latencySamplesNs->reserve((expectedRecords + kLatencySampleMask) / (kLatencySampleMask + 1));
        }

        uint64_t scanned = 0;
        auto rows = reader.scan();
        auto it = rows.begin();
        const auto end = rows.end();
        while (it != end) {
            const bool doSample = ((static_cast<uint32_t>(scanned) & kLatencySampleMask) == 0);
            if (doSample && latencySamplesNs) {
                const auto t0 = Clock::now();
                size_t windowCount = 0;
                while (windowCount < kScanSampleWindow && it != end) {
                    if (it->key.empty()) {
                        std::fprintf(stderr, "SCAN returned an empty key at i=%llu\n", static_cast<unsigned long long>(scanned));
                        std::exit(5);
                    }
                    ++it;
                    ++windowCount;
                    ++scanned;
                }
                const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
                const int64_t perRecordNs = windowCount > 0 ? (dt / static_cast<int64_t>(windowCount)) : 0;
                latencySamplesNs->push_back(static_cast<uint32_t>(std::min<int64_t>(perRecordNs, INT32_MAX)));
                continue;
            }

            if (it->key.empty()) {
                std::fprintf(stderr, "SCAN returned an empty key at i=%llu\n", static_cast<unsigned long long>(scanned));
                std::exit(5);
            }
            ++it;
            ++scanned;
        }
        if (scanned != expectedRecords) {
            std::fprintf(
                stderr,
                "SCAN count mismatch: expected=%zu actual=%llu\n",
                expectedRecords,
                static_cast<unsigned long long>(scanned)
            );
            std::exit(4);
        }
        return scanned;
    }

    [[nodiscard]] static sst::SSTWriter::Options writerOptions(const BenchConfig& config) {
        sst::SSTWriter::Options opts;
        opts.blockSize = config.blockSize;
        opts.codec = config.codec;
        return opts;
    }

    [[nodiscard]] static int resolveCaseOps(CaseSpec spec, const BenchConfig& config) {
        if (config.fixedOps || config.maxCasePayloadBytes == 0) {
            return config.opsPerCase;
        }
        const uint64_t bytesPerOp = static_cast<uint64_t>(spec.keySize) + static_cast<uint64_t>(spec.valueSize);
        if (bytesPerOp == 0) {
            return config.opsPerCase;
        }
        const uint64_t capped = std::max<uint64_t>(1000, config.maxCasePayloadBytes / bytesPerOp);
        return static_cast<int>(std::min<uint64_t>(static_cast<uint64_t>(config.opsPerCase), capped));
    }

    [[nodiscard]] static ThroughputResult runCase(CaseSpec spec, int opsPerCase, const BenchConfig& config, int readerThreads) {
        const int warmupOps = std::min(opsPerCase, 50000);
        const auto warmupDir = makeTempDir(std::format("warmupK{}_v{}", spec.keySize, spec.valueSize));
        {
            auto warmupRecords = makeRecords(spec, warmupOps);
            const auto warmupPath = warmupDir / "warmup.aksst";
            (void)sst::SSTWriter::write(warmupPath, warmupRecords.views, writerOptions(config));
            auto warmupReader = sst::SSTReader::open(warmupPath, sst::SSTReader::Options{config.blockCacheBytes});
            if (!warmupReader) {
                std::fprintf(stderr, "failed to open warmup SST\n");
                std::exit(7);
            }
            runGetParallel(*warmupReader, warmupRecords.keys, readerThreads, nullptr);
            auto warmupScan = warmupReader->scan();
            for (auto&& row : warmupScan) {
                (void)row;
            }
            warmupReader.reset();
        }
        removeTempDir(warmupDir);

        auto records = makeRecords(spec, opsPerCase);
        const auto dir = makeTempDir(std::format("k{}_v{}_r{}", spec.keySize, spec.valueSize, readerThreads));
        const auto path = dir / "bench.aksst";

        const auto writeT0 = Clock::now();
        const auto writeResult = sst::SSTWriter::write(path, records.views, writerOptions(config));
        const auto writeMs = std::chrono::duration<double, std::milli>(Clock::now() - writeT0).count();

        const auto openT0 = Clock::now();
        auto reader = sst::SSTReader::open(path, sst::SSTReader::Options{config.blockCacheBytes});
        const auto openMs = std::chrono::duration<double, std::milli>(Clock::now() - openT0).count();
        if (!reader) {
            std::fprintf(stderr, "failed to open SST: %s\n", path.string().c_str());
            std::exit(8);
        }

        std::vector<uint32_t> getLatencyNs;
        const auto getT0 = Clock::now();
        runGetParallel(*reader, records.keys, readerThreads, &getLatencyNs);
        const auto getMs = std::chrono::duration<double, std::milli>(Clock::now() - getT0).count();

        std::vector<uint32_t> scanLatencyNs;
        const auto scanT0 = Clock::now();
        const uint64_t scanned = runScan(*reader, records.keys.size(), &scanLatencyNs);
        const auto scanMs = std::chrono::duration<double, std::milli>(Clock::now() - scanT0).count();

        reader.reset();
        removeTempDir(dir);

        return {
            .ops = opsPerCase,
            .writeOpsPerSec = static_cast<double>(opsPerCase) * 1000.0 / writeMs,
            .openMs = openMs,
            .getOpsPerSec = static_cast<double>(opsPerCase) * 1000.0 / getMs,
            .scanOpsPerSec = static_cast<double>(scanned) * 1000.0 / scanMs,
            .writeMs = writeMs,
            .getMs = getMs,
            .scanMs = scanMs,
            .fileBytes = writeResult.fileSizeBytes,
            .scanRecords = scanned,
            .getLatency = buildPercentiles(getLatencyNs),
            .scanLatency = buildPercentiles(scanLatencyNs)
        };
    }

    [[nodiscard]] static const char* codecName(sst::SSTWriter::Codec codec) {
        switch (codec) {
            case sst::SSTWriter::Codec::NONE:
                return "none";
            case sst::SSTWriter::Codec::ZSTD:
                return "zstd";
        }
        return "unknown";
    }
} // namespace

int main(int argc, char** argv) {
    akkaradb::test::installMsvcTestErrorHandlers();

    BenchConfig config;
    fs::path exePath = (argc > 0 && argv[0] != nullptr) ? fs::path{argv[0]} : fs::path{};

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg.rfind("--readers=", 0) == 0 || arg.rfind("--writers=", 0) == 0) {
            config.readerThreads = std::max(0, std::atoi(arg.substr(10).c_str()));
            continue;
        }
        if (arg.rfind("--codec=", 0) == 0) {
            const std::string codec = arg.substr(8);
            if (codec == "none") {
                config.codec = sst::SSTWriter::Codec::NONE;
                continue;
            }
            if (codec == "zstd") {
                config.codec = sst::SSTWriter::Codec::ZSTD;
                continue;
            }
            std::fprintf(stderr, "Unknown codec: %s (use none|zstd)\n", codec.c_str());
            return 2;
        }
        if (arg.rfind("--block-size=", 0) == 0) {
            uint64_t parsed = 0;
            if (!parseU64WithSuffix(arg.substr(13), &parsed) || parsed > std::numeric_limits<uint32_t>::max()) {
                std::fprintf(stderr, "Invalid --block-size value: %s\n", arg.substr(13).c_str());
                return 2;
            }
            config.blockSize = static_cast<uint32_t>(parsed);
            continue;
        }
        if (arg.rfind("--cache-bytes=", 0) == 0) {
            uint64_t parsed = 0;
            if (!parseU64WithSuffix(arg.substr(14), &parsed)) {
                std::fprintf(stderr, "Invalid --cache-bytes value: %s\n", arg.substr(14).c_str());
                return 2;
            }
            config.blockCacheBytes = parsed;
            continue;
        }
        if (arg.rfind("--max-case-bytes=", 0) == 0) {
            uint64_t parsed = 0;
            if (!parseU64WithSuffix(arg.substr(17), &parsed)) {
                std::fprintf(stderr, "Invalid --max-case-bytes value: %s\n", arg.substr(17).c_str());
                return 2;
            }
            config.maxCasePayloadBytes = parsed;
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
        if (arg == "--fixed-ops" || arg == "--same-ops") {
            config.fixedOps = true;
            continue;
        }
        config.opsPerCase = std::max(1, std::atoi(arg.c_str()));
    }

    if (config.outputPath.empty()) {
        const fs::path baseDir = exePath.has_parent_path() ? exePath.parent_path() : fs::current_path();
        config.outputPath = (baseDir / "sstable_throughput_results.csv").string();
    }

    const std::array<CaseSpec, 6> cases{{
        {8, 16},
        {16, 64},
        {16, 256},
        {32, 1024},
        {32, 4096},
        {64, 16384}
    }};

    const int effectiveReaders = resolveReaderThreads(config.readerThreads);
    const SystemProfile systemProfile = collectSystemProfile(config.outputPath);

    std::printf("SSTable throughput benchmark\n");
    std::printf("opsPerCase = %d\n", config.opsPerCase);
    std::printf("repeats    = %d\n", config.repeats);
    std::printf("codec = %s\n", codecName(config.codec));
    std::printf("blockSize = %u\n", config.blockSize);
    std::printf("blockCacheBytes = %.2f MiB\n", bytesToMib(config.blockCacheBytes));
    if (config.fixedOps || config.maxCasePayloadBytes == 0) {
        std::printf("maxCasePayloadBytes = disabled (--fixed-ops/--same-ops)\n");
    } else {
        std::printf("maxCasePayloadBytes = %.2f MiB\n", bytesToMib(config.maxCasePayloadBytes));
    }
    if (config.readerThreads == 0) {
        std::printf("readerThreads = auto (%d from hwThreads)\n", effectiveReaders);
    } else {
        std::printf("readerThreads = %d\n", config.readerThreads);
    }
    std::printf("warmupOps = %d\n", std::min(config.opsPerCase, 50000));
    std::printf("outputPath = %s\n\n", config.outputPath.c_str());
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
        std::printf("disk probe = OK, file=%s, size=%s, runs=%u\n\n",
                    systemProfile.disk.tempFilePath.c_str(),
                    formatBytes(systemProfile.disk.fileSizeBytes).c_str(),
                    systemProfile.disk.runs);
    } else if (!systemProfile.disk.error.empty()) {
        std::printf("disk probe = FAILED (%s)\n\n", systemProfile.disk.error.c_str());
    }
    std::printf("%-10s %-12s %-10s %-8s %-14s %-14s %-14s %-12s %-8s %-8s\n",
                "key", "value", "ops", "readers", "write(ops/s)", "get(ops/s)", "scan(ops/s)", "sstBytes", "getSmp", "scanSmp");
    std::printf("%-10s %-12s %-10s %-8s %-14s %-14s %-14s %-12s %-8s %-8s\n",
                "", "", "", "", "", "", "", "", "P50/P90/P99/P999(us)", "P50/P90/P99/P999(us)");
    std::printf("------------------------------------------------------------------------------------------------\n");

    std::vector<CaseReport> reports;
    reports.reserve(cases.size());
    for (const auto& spec : cases) {
        const int caseOps = resolveCaseOps(spec, config);
        std::vector<ThroughputResult> runs;
        runs.reserve(static_cast<size_t>(config.repeats));
        for (int repeat = 0; repeat < config.repeats; ++repeat) {
            runs.push_back(runCase(spec, caseOps, config, effectiveReaders));
        }
        const ThroughputResult result = averageResults(runs);
        reports.push_back(CaseReport{
            .spec = spec,
            .averaged = result,
            .stats = computeResultStats(runs)
        });
        std::printf("%-10d %-12d %-10d %-8d %-14.0f %-14.0f %-14.0f %-12llu %-8u %-8u\n",
                    spec.keySize,
                    spec.valueSize,
                    result.ops,
                    effectiveReaders,
                    result.writeOpsPerSec,
                    result.getOpsPerSec,
                    result.scanOpsPerSec,
                    static_cast<unsigned long long>(result.fileBytes),
                    result.getLatency.sampleCount,
                    result.scanLatency.sampleCount);
        std::printf("%-10s %-12s %-10s %-8s %-14s %-14s %-14s %-12s %4.2f/%4.2f/%4.2f/%4.2f %4.2f/%4.2f/%4.2f/%4.2f\n",
                    "", "", "", "", "", "", "", "",
                    result.getLatency.p50Us,
                    result.getLatency.p90Us,
                    result.getLatency.p99Us,
                    result.getLatency.p999Us,
                    result.scanLatency.p50Us,
                    result.scanLatency.p90Us,
                    result.scanLatency.p99Us,
                    result.scanLatency.p999Us);
        std::printf("  timings(ms): write=%8.2f open=%8.2f get=%8.2f scan=%8.2f   file(MiB/s): write=%8.2f get=%8.2f scan=%8.2f   payload(MiB/s): write=%8.2f get=%8.2f scan=%8.2f\n",
                    result.writeMs,
                    result.openMs,
                    result.getMs,
                    result.scanMs,
                    mibPerSec(result.fileBytes, result.writeMs),
                    mibPerSec(result.fileBytes, result.getMs),
                    mibPerSec(result.fileBytes, result.scanMs),
                    payloadMibPerSec(static_cast<size_t>(spec.keySize + spec.valueSize), result.ops, result.writeMs),
                    payloadMibPerSec(static_cast<size_t>(spec.keySize + spec.valueSize), result.ops, result.getMs),
                    payloadMibPerSec(static_cast<size_t>(spec.keySize + spec.valueSize), result.ops, result.scanMs));
        std::printf("------------------------------------------------------------------------------------------------\n");
        std::fflush(stdout);
    }

    writeReportsCsv(config.outputPath, config, systemProfile, effectiveReaders, codecName(config.codec), reports);

    return 0;
}

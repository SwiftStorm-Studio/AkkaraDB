/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */


// Included inside the anonymous namespace in RaftConsensusRuntime.cpp.
using Clock = std::chrono::steady_clock;
constexpr size_t MAX_RAFT_PENDING_HANDSHAKES = 64;
constexpr size_t MAX_RAFT_CONTROL_CHANNELS = 16;
constexpr uint64_t MIN_RAFT_RECEIVE_BYTES = 3ull * (ReplFrameHeader::SIZE + ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE);

class RaftReceiveBudget : public std::enable_shared_from_this<RaftReceiveBudget> {
    struct Lease {
        Lease(std::shared_ptr<RaftReceiveBudget> ownerValue, uint64_t amountValue) : owner{std::move(ownerValue)}, amount{amountValue} {}
        std::shared_ptr<RaftReceiveBudget> owner;
        uint64_t amount = 0;
        std::shared_ptr<void> nodeLease;
        ~Lease() { owner->used_.fetch_sub(amount, std::memory_order_acq_rel); }
    };

    public:
        explicit RaftReceiveBudget(uint64_t limit, std::shared_ptr<detail::NodeResourceQuota> nodeQuota = {})
            : limit_{limit}, nodeQuota_{std::move(nodeQuota)} {}

        [[nodiscard]] std::shared_ptr<void> reserve(uint64_t amount) {
            auto nodeLease = nodeQuota_ ? nodeQuota_->reserve(amount) : std::shared_ptr<void>{};
            uint64_t current = used_.load(std::memory_order_relaxed);
            while (true) {
                if (amount > limit_ || current > limit_ - amount) { throw std::runtime_error("Raft transport receive-memory budget exhausted"); }
                if (used_.compare_exchange_weak(current, current + amount, std::memory_order_acq_rel, std::memory_order_relaxed)) { break; }
            }
            try {
                auto lease = std::make_shared<Lease>(shared_from_this(), amount);
                lease->nodeLease = std::move(nodeLease);
                return lease;
            }
            catch (...) {
                used_.fetch_sub(amount, std::memory_order_acq_rel);
                throw;
            }
        }

        [[nodiscard]] uint64_t used() const noexcept { return used_.load(std::memory_order_relaxed); }

    private:
        uint64_t limit_ = 0;
        std::atomic<uint64_t> used_{0};
        std::shared_ptr<detail::NodeResourceQuota> nodeQuota_;
};

uint64_t observationNowUs() noexcept {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
}

struct AtomicLatencyHistogram {
    std::array<std::atomic<uint64_t>, 8> bucketCounts{};
    std::atomic<uint64_t> sampleCount{0};
    std::atomic<uint64_t> totalUs{0};
    std::atomic<uint64_t> maxUs{0};

    void observe(uint64_t latencyUs) noexcept {
        ClusterLatencyHistogram layout;
        ++sampleCount;
        totalUs.fetch_add(latencyUs, std::memory_order_relaxed);
        auto previousMax = maxUs.load(std::memory_order_relaxed);
        while (previousMax < latencyUs && !maxUs.compare_exchange_weak(previousMax, latencyUs, std::memory_order_relaxed)) {}
        for (size_t i = 0; i < layout.bucketUpperBoundsUs.size(); ++i) {
            if (latencyUs <= layout.bucketUpperBoundsUs[i]) { bucketCounts[i].fetch_add(1, std::memory_order_relaxed); }
        }
    }

    [[nodiscard]] ClusterLatencyHistogram snapshot() const noexcept {
        ClusterLatencyHistogram out;
        out.sampleCount = sampleCount.load(std::memory_order_relaxed);
        out.totalUs = totalUs.load(std::memory_order_relaxed);
        out.maxUs = maxUs.load(std::memory_order_relaxed);
        for (size_t i = 0; i < out.bucketCounts.size(); ++i) { out.bucketCounts[i] = bucketCounts[i].load(std::memory_order_relaxed); }
        return out;
    }
};

void crashAtLogTestPoint(const char* point) noexcept {
    const char* requested = std::getenv("AKKARADB_TEST_CRASH_POINT");
    if (requested != nullptr && std::strcmp(requested, point) == 0) { std::quick_exit(86); }
}

uint64_t requestJournalCompactRecords() noexcept {
    const char* requested = std::getenv("AKKARADB_TEST_REQUEST_JOURNAL_COMPACT_RECORDS");
    if (requested == nullptr || *requested == '\0') { return 0; }
    char* end = nullptr;
    const auto value = std::strtoull(requested, &end, 10);
    return end != requested && *end == '\0' && value > 0 ? value : 0;
}

#ifdef _WIN32
using SocketHandle = SOCKET;
constexpr auto BAD_SOCKET = INVALID_SOCKET;

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

bool acceptInterrupted() noexcept {
    const auto err = ::WSAGetLastError();
    return err == WSAEINTR || err == WSAECONNABORTED || err == WSAECONNRESET;
}
#else
using SocketHandle = int; constexpr SocketHandle BAD_SOCKET = -1; void netInit() {} void closeSocket(SocketHandle s) noexcept {
    if (s >= 0) { ::close(s); }
} void shutdownSocket(SocketHandle s) noexcept { if (s >= 0) { ::shutdown(s, SHUT_RDWR); } } bool socketOk(SocketHandle s) noexcept { return s >= 0; }
bool acceptInterrupted() noexcept { return errno == EINTR || errno == ECONNABORTED; }
#endif

void writeU8(std::vector<uint8_t>& out, uint8_t value) { out.push_back(value); }

void writeU32(std::vector<uint8_t>& out, uint32_t value) { for (size_t i = 0; i < 4; ++i) { out.push_back(static_cast<uint8_t>(value >> (8 * i))); } }

void writeU64(std::vector<uint8_t>& out, uint64_t value) { for (size_t i = 0; i < 8; ++i) { out.push_back(static_cast<uint8_t>(value >> (8 * i))); } }

void writeU32At(std::vector<uint8_t>& out, size_t off, uint32_t value) {
    for (size_t i = 0; i < 4; ++i) { out[off + i] = static_cast<uint8_t>(value >> (8 * i)); }
}

uint32_t readU32(std::span<const uint8_t> in, size_t off) {
    return static_cast<uint32_t>(in[off]) | (static_cast<uint32_t>(in[off + 1]) << 8) | (static_cast<uint32_t>(in[off + 2]) << 16) | (static_cast<
        uint32_t>(in[off + 3]) << 24);
}

uint64_t readU64(std::span<const uint8_t> in, size_t off) {
    uint64_t out = 0;
    for (size_t i = 0; i < 8; ++i) { out |= static_cast<uint64_t>(in[off + i]) << (8 * i); }
    return out;
}

bool readBytes(std::span<const uint8_t> in, size_t& cursor, size_t len, std::vector<uint8_t>& out) {
    if (cursor + len > in.size()) { return false; }
    out.assign_range(in.subspan(cursor, len));
    cursor += len;
    return true;
}

uint32_t crcBytes(std::span<const uint8_t> bytes) { return cpu::CRC32C(reinterpret_cast<const std::byte*>(bytes.data()), bytes.size()); }

class Crc32cStream {
    public:
        void update(std::span<const uint8_t> bytes) noexcept { for (const auto byte : bytes) { crc_ = (crc_ >> 8u) ^ table()[(crc_ ^ byte) & 0xffu]; } }

        [[nodiscard]] uint32_t finish() const noexcept { return ~crc_; }

    private:
        static const std::array<uint32_t, 256>& table() noexcept {
            static constexpr std::array<uint32_t, 256> values = [] {
                std::array<uint32_t, 256> out{};
                for (uint32_t i = 0; i < out.size(); ++i) {
                    uint32_t crc = i;
                    for (uint32_t bit = 0; bit < 8; ++bit) { crc = (crc >> 1u) ^ (0x82F63B78u & (0u - (crc & 1u))); }
                    out[i] = crc;
                }
                return out;
            }();
            return values;
        }

        uint32_t crc_ = 0xFFFFFFFFu;
};

uint32_t crcWithZeroedField(std::vector<uint8_t> bytes, size_t crcOffset) {
    if (crcOffset + 4 > bytes.size()) { throw std::runtime_error("RaftConsensusRuntime: invalid CRC field"); }
    writeU32At(bytes, crcOffset, 0);
    return crcBytes(bytes);
}

std::vector<uint8_t> readWholeFile(const std::filesystem::path& path, const char* context) {
    std::ifstream in(path, std::ios::binary);
    if (!in) { throw std::runtime_error(std::string{context} + ": cannot open file"); }
    return {std::istreambuf_iterator(in), std::istreambuf_iterator<char>()};
}

std::filesystem::path makeTempPath(const std::filesystem::path& path) {
    static std::atomic<uint64_t> sequence{0};
    const auto parent = path.parent_path();
    const auto stem = path.filename().string();
    #ifdef _WIN32
    const auto pid = static_cast<uint64_t>(::GetCurrentProcessId());
    #else
    const auto pid = static_cast<uint64_t>(::getpid());
    #endif
    for (uint32_t attempt = 0; attempt < 1024; ++attempt) {
        const auto suffix = ".tmp." + std::to_string(pid) + "." + std::to_string(sequence.fetch_add(1)) + "." + std::to_string(attempt);
        auto candidate = parent / (stem + suffix);
        if (!std::filesystem::exists(candidate)) { return candidate; }
    }
    throw std::runtime_error("RaftConsensusRuntime: cannot allocate temp file name");
}

#ifndef _WIN32
void syncParentDirectory(const std::filesystem::path& path, const char* context) {
    const auto parent = path.parent_path().empty() ? std::filesystem::path{"."} : path.parent_path();
    int flags = O_RDONLY;
#ifdef O_DIRECTORY
flags|= O_DIRECTORY;
#endif
const auto fd = ::open(parent.c_str(), flags);if (fd<0) { throw std::runtime_error(std::string{context} + ": cannot open parent directory for sync"); }
const auto rc = ::fsync(fd); const auto closeRc = ::close(fd);if (rc!= 0 || closeRc!= 0) {
        throw std::runtime_error(std::string{context} + ": parent directory sync failed"); }
    }
#endif

void replaceFileAtomically(const std::filesystem::path& tmp, const std::filesystem::path& path, const char* context) {
    #ifdef _WIN32
    if (!MoveFileExW(tmp.wstring().c_str(), path.wstring().c_str(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
        throw std::runtime_error(std::string{context} + ": atomic file replace failed");
    }
    #else
    std::filesystem::rename(tmp, path); syncParentDirectory(path, context);
    #endif
}

void writeFileAtomically(const std::filesystem::path& path, std::span<const uint8_t> bytes, const char* context) {
    if (path.has_parent_path()) { std::filesystem::create_directories(path.parent_path()); }
    const auto tmp = makeTempPath(path);
    {
        std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
        if (!out) { throw std::runtime_error(std::string{context} + ": cannot create temp file"); }
        out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
        out.flush();
        if (!out) { throw std::runtime_error(std::string{context} + ": write failed"); }
    }
    #ifdef _WIN32
    const auto fd = _wopen(tmp.c_str(), _O_RDWR | _O_BINARY);
    if (fd < 0) { throw std::runtime_error(std::string{context} + ": cannot reopen temp file for sync"); }
    const auto rc = _commit(fd);
    const auto closeRc = _close(fd);
    if (rc != 0 || closeRc != 0) { throw std::runtime_error(std::string{context} + ": temp file sync failed"); }
    #else
    const auto fd = ::open(tmp.c_str(), O_RDONLY); if (fd < 0) { throw std::runtime_error(std::string{context} + ": cannot reopen temp file for sync"); }
    const auto rc = ::fsync(fd); const auto closeRc = ::close(fd); if (rc != 0 || closeRc != 0) {
        throw std::runtime_error(std::string{context} + ": temp file sync failed");
    }
    #endif
    replaceFileAtomically(tmp, path, context);
}


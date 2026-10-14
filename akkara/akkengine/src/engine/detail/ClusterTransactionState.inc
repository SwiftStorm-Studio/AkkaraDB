/* AkkaraDB - Copyright (C) 2026 Swift Storm Studio
 * SPDX-License-Identifier: MPL-2.0 */
// Included inside AkkEngine::Impl.
using DistributedId = cluster::ClusterTransactionId;
using DistributedMessage = cluster::detail::TransactionMessage;
using DistributedAction = cluster::detail::TransactionAction;
using DistributedStatus = cluster::ClusterTransactionStatus;
using DistributedRecord = cluster::detail::TransactionRecord;
using DistributedPrepare = cluster::detail::TransactionPrepare;

struct DistributedParticipant {
    struct Chunk {
        uint64_t sequence = 0;
        uint32_t size = 0;
        std::array<uint8_t, 32> digest{};
        std::shared_ptr<const std::vector<uint8_t>> data;
    };
    DistributedId id{};
    uint64_t expires = 0, expectedBytes = 0, timestamp = 0, blobSequence = 0, applyingSequence = 0;
    uint64_t receivedBytes = 0;
    bool frozen = false, sealed = false, applied = false, queryBarrier = false;
    std::vector<Chunk> chunks;
    std::set<std::vector<uint8_t>> keys;
    std::vector<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>> ranges;
};
static bool distributedKeyLocked(const DistributedParticipant& p, std::span<const uint8_t> key) {
    if (p.keys.contains(std::vector<uint8_t>{key.begin(), key.end()})) { return true; }
    return std::ranges::any_of(p.ranges, [&](const auto& range) {
        return compareKey(key, range.first) >= 0 && (range.second.empty() || compareKey(key, range.second) < 0);
    });
}
struct DistributedState {
    uint64_t sequence = 0;
    uint64_t deltaGeneration = 0;
    std::map<DistributedId, DistributedRecord> decisions;
    uint64_t decisionGeneration = 0, completionGeneration = 0, decisionFloor = 0, completionFloor = 0;
    uint64_t decisionCount = 0, completionCount = 0;
    std::map<DistributedId, DistributedParticipant> participants;
};
mutable std::mutex distributedStateMu;
std::condition_variable distributedStateCv;
std::timed_mutex distributedProposalMu;
std::timed_mutex distributedCoordinatorMu;
DistributedState distributedState;
uint64_t distributedCheckpointGeneration = 0;
std::shared_ptr<cluster::detail::DiskResults> distributedDecisions, distributedCompletions;
std::jthread distributedRecoveryThread;
struct DistributedRecoveryDiagnostics {
    uint64_t failuresTotal = 0, consecutiveFailedPasses = 0;
    uint64_t lastFailureAtUs = 0, failingSinceUs = 0, lastSuccessfulPassAtUs = 0;
    DistributedId lastFailedId{};
    std::array<char, 512> lastError{};
    size_t lastErrorSize = 0;
};
mutable std::mutex distributedRecoveryDiagnosticsMu;
DistributedRecoveryDiagnostics distributedRecoveryDiagnostics;
void recordDistributedRecoveryFailure(const DistributedId& id, const char* error) {
    std::lock_guard lock{distributedRecoveryDiagnosticsMu};
    auto& d = distributedRecoveryDiagnostics;
    ++d.failuresTotal; d.lastFailureAtUs = nowNs() / 1000;
    if (!d.failingSinceUs) { d.failingSinceUs = d.lastFailureAtUs; }
    d.lastFailedId = id; d.lastErrorSize = std::min<size_t>(std::char_traits<char>::length(error), d.lastError.size());
    std::copy_n(error, d.lastErrorSize, d.lastError.begin());
}
void finishDistributedRecoveryPass(bool failed) {
    std::lock_guard lock{distributedRecoveryDiagnosticsMu};
    auto& d = distributedRecoveryDiagnostics;
    if (failed) { ++d.consecutiveFailedPasses; }
    else { d.consecutiveFailedPasses = 0; d.failingSinceUs = 0; d.lastSuccessfulPassAtUs = nowNs() / 1000; }
}

bool distributedTransactionsEnabled() const noexcept {
    return opts.components.clusterEnabled && opts.cluster.runtime.transactions.mode == cluster::ClusterTransactionMode::TWO_PHASE_COMMIT;
}
static bool distributedIdPresent(const DistributedId& id) { return std::ranges::any_of(id, [](auto byte) { return byte != 0; }); }
static uint64_t distributedNowMs() { return nowNs() / 1'000'000; }
fs::path distributedStatePath() const { return opts.paths.transactionDir / "cluster-state.akdt"; }
static std::vector<uint8_t> encodeDistributedState(const DistributedState& state) {
    using namespace cluster::detail;
    std::vector<uint8_t> bytes{'A','K','D','S','7'};
    for (auto value : {state.sequence, state.decisionGeneration, state.completionGeneration, state.decisionFloor,
        state.completionFloor, state.decisionCount, state.completionCount}) { transactionInteger(bytes, value); }
    transactionInteger(bytes, state.deltaGeneration);
    transactionInteger(bytes, state.decisions.size(), 4);
    for (const auto& [id, record] : state.decisions) {
        bytes.insert(bytes.end(), id.begin(), id.end()); bytes.push_back(static_cast<uint8_t>(record.status));
        bytes.push_back(static_cast<uint8_t>((record.finished ? 1 : 0) | (record.ready ? 2 : 0))); transactionInteger(bytes, record.expires);
        transactionInteger(bytes, record.participants.size(), 4);
        for (const auto participant : record.participants) { transactionInteger(bytes, participant, 2); }
    }
    transactionInteger(bytes, state.participants.size(), 4);
    for (const auto& [id, p] : state.participants) {
        bytes.insert(bytes.end(), p.id.begin(), p.id.end());
        transactionInteger(bytes, p.expires); transactionInteger(bytes, p.expectedBytes);
        transactionInteger(bytes, p.timestamp); transactionInteger(bytes, p.blobSequence); transactionInteger(bytes, p.applyingSequence);
        bytes.push_back(static_cast<uint8_t>((p.frozen ? 1 : 0) | (p.sealed ? 2 : 0) | (p.applied ? 4 : 0) | (p.queryBarrier ? 8 : 0)));
        transactionInteger(bytes, p.receivedBytes);
        transactionInteger(bytes, p.chunks.size(), 4);
        for (const auto& chunk : p.chunks) {
            transactionInteger(bytes, chunk.sequence); transactionInteger(bytes, chunk.size, 4);
            bytes.insert(bytes.end(), chunk.digest.begin(), chunk.digest.end());
        }
        transactionInteger(bytes, p.keys.size(), 4);
        for (const auto& key : p.keys) { transactionBytes(bytes, key); }
        transactionInteger(bytes, p.ranges.size(), 4);
        for (const auto& range : p.ranges) { transactionBytes(bytes, range.first); transactionBytes(bytes, range.second); }
    }
    transactionInteger(bytes, blob::crc32c(bytes), 4); return bytes;
}
static DistributedState decodeDistributedState(std::span<const uint8_t> bytes) {
    using namespace cluster::detail;
    if (bytes.size() < 81 || !std::equal(bytes.begin(), bytes.begin() + 5, "AKDS7")) { throw std::runtime_error("Cluster transaction: invalid durable state"); }
    TransactionReader checksum{bytes.last(4)};
    if (blob::crc32c(bytes.first(bytes.size() - 4)) != checksum.integer(4)) { throw std::runtime_error("Cluster transaction: corrupt durable state"); }
    TransactionReader reader{bytes.subspan(5, bytes.size() - 9)};
    DistributedState state; state.sequence = reader.integer();
    state.decisionGeneration = reader.integer(); state.completionGeneration = reader.integer();
    state.decisionFloor = reader.integer(); state.completionFloor = reader.integer();
    state.decisionCount = reader.integer(); state.completionCount = reader.integer();
    state.deltaGeneration = reader.integer();
    const auto records = reader.count(30);
    for (uint32_t i = 0; i < records; ++i) {
        DistributedId id{}; const auto token = reader.take(id.size()); std::copy(token.begin(), token.end(), id.begin());
        const auto phase = reader.integer(1), finished = reader.integer(1);
        if (!distributedIdPresent(id) || phase < static_cast<uint8_t>(DistributedStatus::OPEN) || phase > static_cast<uint8_t>(DistributedStatus::ABORTED) || finished > 3) { throw std::runtime_error("Cluster transaction: malformed decision"); }
        DistributedRecord record; record.status = static_cast<DistributedStatus>(phase); record.finished = (finished & 1) != 0; record.ready = (finished & 2) != 0;
        record.expires = reader.integer(); const auto count = reader.count(2);
        if (count > 256 || !record.expires) { throw std::runtime_error("Cluster transaction: malformed participant list"); }
        for (uint32_t n = 0; n < count; ++n) {
            const auto participant = static_cast<uint16_t>(reader.integer(2));
            if (n && participant <= record.participants.back()) { throw std::runtime_error("Cluster transaction: unordered participants"); }
            record.participants.push_back(participant);
        }
        if (record.finished) { throw std::runtime_error("Cluster transaction: completed decision in active state"); }
        if (!state.decisions.emplace(id, std::move(record)).second) { throw std::runtime_error("Cluster transaction: duplicate decision"); }
    }
    const auto participants = reader.count(77);
    for (uint32_t n = 0; n < participants; ++n) {
        DistributedParticipant p;
        const auto token = reader.take(p.id.size()); std::copy(token.begin(), token.end(), p.id.begin());
        p.expires = reader.integer(); p.expectedBytes = reader.integer(); p.timestamp = reader.integer(); p.blobSequence = reader.integer(); p.applyingSequence = reader.integer();
        const auto flags = reader.integer(1);
        if (flags > 15) { throw std::runtime_error("Cluster transaction: invalid participant flags"); }
        p.frozen = (flags & 1) != 0; p.sealed = (flags & 2) != 0; p.applied = (flags & 4) != 0; p.queryBarrier = (flags & 8) != 0;
        p.receivedBytes = reader.integer();
        const auto chunks = reader.count(44);
        uint64_t received = 0;
        for (uint32_t i = 0; i < chunks; ++i) {
            DistributedParticipant::Chunk chunk; chunk.sequence = reader.integer(); chunk.size = static_cast<uint32_t>(reader.integer(4));
            const auto digest = reader.take(chunk.digest.size()); std::copy(digest.begin(), digest.end(), chunk.digest.begin());
            if (!chunk.sequence || chunk.sequence > state.sequence || !chunk.size || chunk.size > 1024 * 1024 ||
                (!p.chunks.empty() && chunk.sequence <= p.chunks.back().sequence)) { throw std::runtime_error("Cluster transaction: invalid prepare chunk descriptor"); }
            received += chunk.size; p.chunks.push_back(std::move(chunk));
        }
        const auto keys = reader.count(4);
        for (uint32_t i = 0; i < keys; ++i) {
            auto key = reader.field();
            if (key.size() > UINT16_MAX || !p.keys.insert(std::move(key)).second) { throw std::runtime_error("Cluster transaction: invalid key reservation"); }
        }
        const auto ranges = reader.count(8);
        for (uint32_t i = 0; i < ranges; ++i) {
            auto start = reader.field(), end = reader.field();
            if (start.size() > UINT16_MAX || end.size() > UINT16_MAX || (!end.empty() && start >= end)) { throw std::runtime_error("Cluster transaction: invalid range reservation"); }
            p.ranges.emplace_back(std::move(start), std::move(end));
        }
        if (!distributedIdPresent(p.id) || !p.expires || !p.frozen || received != p.receivedBytes ||
            p.receivedBytes > p.expectedBytes || (p.sealed && p.receivedBytes != p.expectedBytes) || (p.applied && !p.sealed) ||
            (p.queryBarrier && (p.sealed || p.expectedBytes || p.receivedBytes || p.ranges.empty()))) {
            throw std::runtime_error("Cluster transaction: invalid prepared state");
        }
        if (!state.participants.emplace(p.id, std::move(p)).second) { throw std::runtime_error("Cluster transaction: duplicate participant"); }
    }
    reader.finish();
    return state;
}
void persistDistributedState(DistributedState& state) {
    if (distributedCheckpointGeneration == UINT64_MAX) { throw std::overflow_error("Cluster transaction: prepare journal generation exhausted"); }
    state.deltaGeneration = distributedCheckpointGeneration + 1;
    const auto bytes = encodeDistributedState(state);
    if (bytes.size() > opts.transactions.maxJournalBytes) { throw std::length_error("Cluster transaction: durable state capacity exceeded"); }
    writeFileAtomicallyDurable(distributedStatePath(), bytes);
    distributedCheckpointGeneration = state.deltaGeneration;
    if (std::ranges::any_of(state.participants, [](const auto& item) {
        return item.second.sealed && !item.second.applied && !item.second.applyingSequence;
    })) { crashAtTestPoint("cluster_transaction.after_prepare_checkpoint"); }
    pruneDistributedDeltas(state.deltaGeneration);
}
fs::path distributedDeltaDir() const { return opts.paths.transactionDir / "cluster-prepare-deltas"; }
static std::pair<uint64_t, uint64_t> distributedDeltaIdentity(const fs::path& path) {
    const auto name = path.stem().string(); const auto split = name.find('-');
    if (split == std::string::npos) { throw std::runtime_error("Cluster transaction: invalid prepare journal name"); }
    size_t consumed = 0; const auto generation = std::stoull(name.substr(0, split), &consumed);
    if (!generation || consumed != split) { throw std::runtime_error("Cluster transaction: invalid prepare journal generation"); }
    const auto sequence = std::stoull(name.substr(split + 1), &consumed);
    if (!sequence || consumed != name.size() - split - 1) { throw std::runtime_error("Cluster transaction: invalid prepare journal sequence"); }
    return {generation, sequence};
}
void pruneDistributedDeltas(uint64_t generation) {
    const auto directory = distributedDeltaDir();
    if (!fs::exists(directory)) { return; }
    for (const auto& entry : fs::directory_iterator(directory)) {
        if (entry.path().extension() == ".akdd" && distributedDeltaIdentity(entry.path()).first != generation) {
            std::error_code ignored; fs::remove(entry.path(), ignored);
        }
    }
}
void persistDistributedAppend(const DistributedParticipant& p, const DistributedParticipant::Chunk& chunk) {
    using namespace cluster::detail;
    std::vector<uint8_t> bytes{'A','K','D','J','1'};
    transactionInteger(bytes, chunk.sequence); transactionInteger(bytes, distributedState.deltaGeneration);
    bytes.insert(bytes.end(), p.id.begin(), p.id.end());
    transactionInteger(bytes, p.receivedBytes); transactionInteger(bytes, chunk.size, 4);
    bytes.insert(bytes.end(), chunk.digest.begin(), chunk.digest.end());
    transactionInteger(bytes, blob::crc32c(bytes), 4);
    writeFileAtomicallyDurable(distributedDeltaDir() / (std::to_string(distributedState.deltaGeneration) + "-" + std::to_string(chunk.sequence) + ".akdd"), bytes);
}
void recoverDistributedDeltas(DistributedState& state) {
    const auto directory = distributedDeltaDir();
    if (!fs::exists(directory)) { return; }
    std::map<uint64_t, fs::path> records;
    for (const auto& entry : fs::directory_iterator(directory)) {
        if (entry.path().extension() == ".akdd") {
            const auto [generation, sequence] = distributedDeltaIdentity(entry.path());
            if (generation == state.deltaGeneration && sequence > state.sequence && !records.emplace(sequence, entry.path()).second) {
                throw std::runtime_error("Cluster transaction: duplicate prepare journal sequence");
            }
        }
    }
    for (const auto& [sequence, path] : records) {
        if (fs::file_size(path) != 85) { throw std::runtime_error("Cluster transaction: invalid prepare journal length"); }
        std::array<uint8_t, 85> bytes{}; std::ifstream input{path, std::ios::binary};
        if (!input.read(reinterpret_cast<char*>(bytes.data()), bytes.size()) || !std::equal(bytes.begin(), bytes.begin() + 5, "AKDJ1")) {
            throw std::runtime_error("Cluster transaction: unreadable prepare journal");
        }
        cluster::detail::TransactionReader reader{std::span<const uint8_t>{bytes}.subspan(5)};
        const auto storedSequence = reader.integer(), generation = reader.integer(); DistributedId id{};
        const auto token = reader.take(id.size()); std::copy(token.begin(), token.end(), id.begin());
        const auto offset = reader.integer();
        DistributedParticipant::Chunk chunk; chunk.sequence = sequence; chunk.size = static_cast<uint32_t>(reader.integer(4));
        const auto digest = reader.take(chunk.digest.size()); std::copy(digest.begin(), digest.end(), chunk.digest.begin());
        const auto checksum = reader.integer(4); reader.finish();
        const auto it = state.participants.find(id);
        if (storedSequence != sequence || generation != state.deltaGeneration || checksum != blob::crc32c(std::span<const uint8_t>{bytes}.first(81)) ||
            it == state.participants.end() || it->second.sealed || offset != it->second.receivedBytes || !chunk.size || chunk.size > 1024 * 1024 ||
            chunk.size > it->second.expectedBytes - it->second.receivedBytes) { throw std::runtime_error("Cluster transaction: invalid prepare journal delta"); }
        it->second.receivedBytes += chunk.size; it->second.chunks.push_back(std::move(chunk)); state.sequence = sequence;
    }
}
static uint64_t distributedReservedBytes(const DistributedState& state) {
    uint64_t bytes = encodeDistributedState(state).size();
    for (const auto& [id, p] : state.participants) {
        if (p.expectedBytes > UINT64_MAX - bytes) { return UINT64_MAX; }
        bytes += p.expectedBytes;
        // Reserve descriptor space for the normal 1 MiB chunk stream up front.
        const auto planned = p.expectedBytes / (1024 * 1024) + (p.expectedBytes % (1024 * 1024) != 0);
        if (planned > p.chunks.size()) {
            const auto descriptors = (planned - p.chunks.size()) * 44;
            if (descriptors > UINT64_MAX - bytes) { return UINT64_MAX; }
            bytes += descriptors;
        }
    }
    return bytes;
}
static bool distributedRangesOverlap(const auto& left, const auto& right) {
    return (left.second.empty() || compareKey(right.first, left.second) < 0) &&
        (right.second.empty() || compareKey(left.first, right.second) < 0);
}
static bool distributedReservationsOverlap(const DistributedParticipant& left, const DistributedParticipant& right) {
    for (const auto& key : left.keys) { if (distributedKeyLocked(right, key)) { return true; } }
    for (const auto& key : right.keys) { if (distributedKeyLocked(left, key)) { return true; } }
    for (const auto& a : left.ranges) {
        for (const auto& b : right.ranges) { if (distributedRangesOverlap(a, b)) { return true; } }
    }
    return false;
}
static bool distributedReservationConflict(const DistributedState& state, const DistributedParticipant& candidate) {
    return std::ranges::any_of(state.participants, [&](const auto& item) {
        return item.first != candidate.id && distributedReservationsOverlap(candidate, item.second);
    });
}
fs::path distributedChunkPath(const std::array<uint8_t, 32>& digest) const {
    std::string name; name.reserve(69);
    constexpr char hex[] = "0123456789abcdef";
    for (auto byte : digest) { name.push_back(hex[byte >> 4]); name.push_back(hex[byte & 15]); }
    return opts.paths.transactionDir / "cluster-prepare" / (name + ".akdp");
}
static std::array<uint8_t, 32> distributedChunkDigest(std::span<const uint8_t> data) {
    const std::array parts{data}; return crypto::hash256(parts);
}
void saveDistributedChunk(const DistributedParticipant::Chunk& chunk) {
    if (!chunk.data || chunk.data->size() != chunk.size || distributedChunkDigest(*chunk.data) != chunk.digest) {
        throw std::runtime_error("Cluster transaction: invalid prepare chunk");
    }
    std::vector<uint8_t> bytes{'A','K','D','C','1'};
    cluster::detail::transactionInteger(bytes, chunk.size, 4);
    cluster::detail::transactionInteger(bytes, blob::crc32c(*chunk.data), 4);
    bytes.insert(bytes.end(), chunk.data->begin(), chunk.data->end());
    writeFileAtomicallyDurable(distributedChunkPath(chunk.digest), bytes);
}
void loadDistributedChunk(DistributedParticipant::Chunk& chunk) {
    const auto path = distributedChunkPath(chunk.digest);
    std::ifstream input{path, std::ios::binary};
    if (!input || fs::file_size(path) != uint64_t{chunk.size} + 13) { throw std::runtime_error("Cluster transaction: missing prepare chunk"); }
    std::vector<uint8_t> bytes(static_cast<size_t>(chunk.size) + 13);
    if (!input.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size())) ||
        !std::equal(bytes.begin(), bytes.begin() + 5, "AKDC1")) { throw std::runtime_error("Cluster transaction: corrupt prepare chunk"); }
    cluster::detail::TransactionReader header{std::span<const uint8_t>{bytes}.subspan(5, 8)};
    const auto size = header.integer(4), checksum = header.integer(4);
    auto data = std::make_shared<const std::vector<uint8_t>>(bytes.begin() + 13, bytes.end());
    if (size != chunk.size || blob::crc32c(*data) != checksum || distributedChunkDigest(*data) != chunk.digest) {
        throw std::runtime_error("Cluster transaction: prepare chunk integrity failure");
    }
    chunk.data = std::move(data);
}
static std::vector<uint8_t> distributedPrepareBytes(const DistributedParticipant& p) {
    std::vector<uint8_t> bytes; bytes.reserve(static_cast<size_t>(p.receivedBytes));
    for (const auto& chunk : p.chunks) {
        if (!chunk.data) { throw std::runtime_error("Cluster transaction: missing prepared payload"); }
        bytes.insert(bytes.end(), chunk.data->begin(), chunk.data->end());
    }
    return bytes;
}
static bool distributedPrepareMatches(const DistributedParticipant& p, uint64_t offset, std::span<const uint8_t> bytes) {
    if (offset > p.receivedBytes || bytes.size() > p.receivedBytes - offset) { return false; }
    for (const auto& chunk : p.chunks) {
        if (offset >= chunk.size) { offset -= chunk.size; continue; }
        const auto count = std::min<size_t>(bytes.size(), chunk.size - static_cast<size_t>(offset));
        if (!std::equal(bytes.begin(), bytes.begin() + static_cast<ptrdiff_t>(count), chunk.data->begin() + static_cast<ptrdiff_t>(offset))) { return false; }
        bytes = bytes.subspan(count); offset = 0;
        if (bytes.empty()) { return true; }
    }
    return bytes.empty();
}
void validateDistributedWrites(const cluster::detail::TransactionPrepare& prepare) const {
    for (const auto& write : prepare.writes) {
        const auto storedSize = write.value && blobManager && write.value->size() >= blobManager->threshold()
            ? blob::BLOB_REF_SIZE : write.value ? write.value->size() : 0;
        validateStoredRecord(write.key.size(), storedSize);
    }
}
void validateDistributedParticipants(const DistributedState& state) const {
    for (const auto& [id, p] : state.participants) {
        if (distributedReservationConflict(state, p)) { throw std::runtime_error("Cluster transaction: overlapping durable reservations"); }
        for (const auto& chunk : p.chunks) { if (!chunk.data) { throw std::runtime_error("Cluster transaction: incomplete prepared payload"); } }
        if (p.sealed) {
            const auto prepare = cluster::detail::decodeTransactionPrepare(distributedPrepareBytes(p));
            validateDistributedWrites(prepare);
            for (const auto& write : prepare.writes) {
                if (!distributedKeyLocked(p, write.key)) { throw std::runtime_error("Cluster transaction: prepared write lacks a reservation"); }
            }
        }
    }
}
void removeDistributedChunks(const DistributedParticipant& removed) {
    for (const auto& chunk : removed.chunks) {
        const bool retained = std::ranges::any_of(distributedState.participants, [&](const auto& item) {
            return std::ranges::any_of(item.second.chunks, [&](const auto& active) { return active.digest == chunk.digest; });
        });
        if (!retained) { std::error_code ignored; fs::remove(distributedChunkPath(chunk.digest), ignored); }
    }
}
void collectDistributedChunks() {
    const auto directory = opts.paths.transactionDir / "cluster-prepare";
    if (!fs::exists(directory)) { return; }
    std::set<fs::path> retained;
    for (const auto& [id, p] : distributedState.participants) {
        for (const auto& chunk : p.chunks) { retained.insert(distributedChunkPath(chunk.digest)); }
    }
    for (const auto& entry : fs::directory_iterator(directory)) {
        if ((entry.path().extension() == ".akdp" || entry.path().extension() == ".tmp") && !retained.contains(entry.path())) {
            std::error_code ignored; fs::remove(entry.path(), ignored);
        }
    }
}
std::shared_ptr<cluster::detail::DiskResults>& distributedResults(bool coordinator) {
    auto& store = coordinator ? distributedDecisions : distributedCompletions;
    if (!store) { store = std::make_shared<cluster::detail::DiskResults>(opts.paths.transactionDir /
        (coordinator ? "cluster-decisions" : "cluster-receipts"), opts.cluster.runtime.transactions.maxRetainedResultBytes); }
    return store;
}
bool distributedCompleted(const DistributedId& id) {
    return distributedResults(false)->get(id).has_value();
}
void observeDistributedResults(DistributedState& state) {
    if (distributedDecisions) {
        state.decisionGeneration = distributedDecisions->generation(); state.decisionFloor = distributedDecisions->floor();
        state.decisionCount = distributedDecisions->count();
    }
    if (distributedCompletions) {
        state.completionGeneration = distributedCompletions->generation(); state.completionFloor = distributedCompletions->floor();
        state.completionCount = distributedCompletions->count();
    }
}
cluster::ClusterSnapshot exportDistributedSnapshot(uint64_t sequence = 0) {
    std::lock_guard lock{distributedStateMu}; auto state = distributedState;
    if (sequence) { state.sequence = sequence; }
    observeDistributedResults(state);
    auto image = encodeDistributedState(state);
    std::optional<cluster::detail::DiskResults::Snapshot> decisions, completions;
    if (state.decisionGeneration) { decisions = distributedResults(true)->capture(); }
    if (state.completionGeneration) { completions = distributedResults(false)->capture(); }
    return {.seq = state.sequence, .forEachEntry = [image = std::move(image), participants = std::move(state.participants), decisions, completions](const cluster::SnapshotEntryVisitor& visitor) {
        const auto key = cluster::detail::encodeSnapshotKey({cluster::detail::SnapshotRecordKind::TRANSACTION, 0, 0, 0, 0, {}});
        if (!streamSnapshotValue(visitor, key, image, 0, nullptr)) { return false; }
        for (const auto& [id, participant] : participants) {
            uint64_t offset = 0;
            for (const auto& chunk : participant.chunks) {
                const auto encoded = cluster::detail::encodeSnapshotKey({cluster::detail::SnapshotRecordKind::TRANSACTION_PREPARE,
                    0, chunk.sequence, offset, 0, id});
                if (!streamSnapshotValue(visitor, encoded, *chunk.data, 0, nullptr)) { return false; }
                offset += chunk.size;
            }
        }
        const auto send = [&](const auto& source, cluster::detail::SnapshotRecordKind kind) {
            return !source || source->forEach([&](auto id, const cluster::detail::DiskResults::Record& record) {
                const auto encoded = cluster::detail::encodeSnapshotKey({kind, 0, record.sequence, record.expires, 0, id});
                return streamSnapshotValue(visitor, encoded, record.value, 0, nullptr);
            });
        };
        return send(decisions, cluster::detail::SnapshotRecordKind::TRANSACTION_DECISION) &&
            send(completions, cluster::detail::SnapshotRecordKind::TRANSACTION_RECEIPT);
    }};
}
void installDistributedSnapshot(const cluster::ClusterSnapshot& snapshot) {
    std::optional<DistributedState> state;
    std::optional<cluster::detail::DiskResults::Import> decisions, completions;
    std::vector<uint8_t> key, value; uint64_t expected = 0; uint32_t crc = 0;
    cluster::SnapshotEntryVisitor visitor;
    visitor.beginEntry = [&](auto encoded, uint64_t size, uint32_t checksum) {
        if (size > opts.transactions.maxJournalBytes ||
            (cluster::detail::decodeSnapshotKey(encoded).kind == cluster::detail::SnapshotRecordKind::TRANSACTION_PREPARE && size > 1024 * 1024)) {
            throw std::runtime_error("Cluster transaction: oversized snapshot entry");
        }
        key.assign(encoded.begin(), encoded.end()); value.clear(); expected = size; crc = checksum; return true;
    };
    visitor.appendValueChunk = [&](uint64_t offset, auto chunk) {
        if (offset != value.size() || chunk.size() > expected - offset) { throw std::runtime_error("Cluster transaction: invalid snapshot chunk"); }
        value.insert(value.end(), chunk.begin(), chunk.end()); return true;
    };
    visitor.finishEntry = [&] {
        if (value.size() != expected || blob::crc32c(value) != crc) { throw std::runtime_error("Cluster transaction: corrupt snapshot entry"); }
        const auto record = cluster::detail::decodeSnapshotKey(key);
        if (record.kind == cluster::detail::SnapshotRecordKind::TRANSACTION) {
            if (state || !record.key.empty() || record.sequence || record.source || record.flags || record.timestamp) { throw std::runtime_error("Cluster transaction: duplicate snapshot state"); }
            state = decodeDistributedState(value);
            if (state->sequence != snapshot.seq || distributedReservedBytes(*state) > opts.transactions.maxJournalBytes ||
                std::ranges::any_of(state->participants, [](const auto& item) { return item.second.applyingSequence && !item.second.applied; })) {
                throw std::runtime_error("Cluster transaction: snapshot cut mismatch or incomplete application");
            }
            decisions = distributedResults(true)->beginImport(state->sequence, state->decisionFloor);
            completions = distributedResults(false)->beginImport(state->sequence, state->completionFloor);
        } else {
            if (!state || !std::ranges::any_of(record.key, [](auto byte) { return byte != 0; }) || record.key.size() != 16 || !record.sequence || record.sequence > state->sequence || record.flags || record.timestamp || record.blobId) {
                throw std::runtime_error("Cluster transaction: invalid retained snapshot result");
            }
            if (record.kind == cluster::detail::SnapshotRecordKind::TRANSACTION_PREPARE) {
                DistributedId id{}; std::copy(record.key.begin(), record.key.end(), id.begin());
                const auto it = state->participants.find(id);
                if (it == state->participants.end()) { throw std::runtime_error("Cluster transaction: unknown snapshot preparation"); }
                uint64_t offset = 0; bool found = false;
                for (auto& chunk : it->second.chunks) {
                    if (offset == record.source) {
                        if (chunk.data || chunk.sequence != record.sequence || chunk.size != value.size() || distributedChunkDigest(value) != chunk.digest) {
                            throw std::runtime_error("Cluster transaction: invalid snapshot preparation");
                        }
                        chunk.data = std::make_shared<const std::vector<uint8_t>>(std::move(value)); found = true; break;
                    }
                    offset += chunk.size;
                }
                if (!found) { throw std::runtime_error("Cluster transaction: invalid snapshot preparation offset"); }
            } else if (record.kind == cluster::detail::SnapshotRecordKind::TRANSACTION_DECISION && record.source) {
                const auto result = decodeDistributedRecord(value);
                if (!result.finished || result.expires != record.source) { throw std::runtime_error("Cluster transaction: unfinished archived snapshot decision"); }
                distributedResults(true)->import(*decisions, record.key, {record.source, record.sequence, value});
            } else if (record.kind == cluster::detail::SnapshotRecordKind::TRANSACTION_RECEIPT && record.source && value.empty()) {
                distributedResults(false)->import(*completions, record.key, {record.source, record.sequence, {}});
            } else { throw std::runtime_error("Cluster transaction: invalid snapshot result kind"); }
        }
        return true;
    };
    if (!snapshot.forEachEntry(visitor) || !state || decisions->count != state->decisionCount || completions->count != state->completionCount) {
        throw std::runtime_error("Cluster transaction: incomplete retained-result snapshot");
    }
    validateDistributedParticipants(*state);
    for (const auto& [id, p] : state->participants) { for (const auto& chunk : p.chunks) { saveDistributedChunk(chunk); } }
    distributedResults(true)->prepare(*decisions); distributedResults(false)->prepare(*completions);
    state->decisionGeneration = decisions->generation; state->completionGeneration = completions->generation;
    std::lock_guard lock{distributedStateMu}; persistDistributedState(*state);
    distributedResults(true)->activate(decisions->generation); distributedResults(false)->activate(completions->generation);
    distributedState = std::move(*state);
    collectDistributedChunks();
    distributedResults(true)->collectGarbage(); distributedResults(false)->collectGarbage();
}
void applyDistributedWrites(uint64_t sequence, const DistributedParticipant& participant, bool recovering) {
    const auto prepare = cluster::detail::decodeTransactionPrepare(distributedPrepareBytes(participant));
    validateDistributedWrites(prepare);
    uint64_t source = 0;
    for (unsigned i = 0; i < 8; ++i) { source |= uint64_t{participant.id[i]} << (8 * i); }
    std::unique_lock planning{blobGcPlanMu};
    transactionJournalPending.store(true, std::memory_order_release);
    memtable->advanceSeq(sequence);
    uint64_t ordinal = 0;
    for (const auto& write : prepare.writes) {
        const auto position = ordinal++;
        uint8_t flags = write.value ? MemHdr16::FLAG_NORMAL : MemHdr16::FLAG_TOMBSTONE;
        auto value = write.value ? *write.value : std::vector<uint8_t>{};
        if (write.value && blobManager && value.size() >= blobManager->threshold()) {
            if (participant.blobSequence == 0 || participant.blobSequence >= (1ull << 46) || position >= 65536) { throw std::overflow_error("Cluster transaction: Blob identity exhausted"); }
            const auto blobId = (3ull << 62) | (participant.blobSequence << 16) | position;
            blobManager->write(blobId, value);
            std::vector<uint8_t> ref(blob::BLOB_REF_SIZE);
            blob::encodeBlobRef(ref.data(), blob::BlobRef{blobId, static_cast<uint64_t>(value.size()), blob::crc32c(value)});
            value = std::move(ref); flags |= MemHdr16::FLAG_BLOB;
        }
        if (walWriter) { walWriter->append(write.key, value, sequence, flags, 0, wal::WalAppendAck::WRITTEN); }
        if (versionLog && (!recovering || !versionLog->containsStoredRecord(write.key, sequence, source, participant.timestamp, flags))) {
            versionLog->appendDeferred(write.key, sequence, source, participant.timestamp, flags, value);
        }
        const auto current = memtable->getPinned(write.key, UINT64_MAX);
        bool replace = !current || current->view.seq() <= sequence;
        if (replace && sstManager) { const auto record = sstManager->get(write.key); if (record && record->seq > sequence) { replace = false; } }
        if (replace) {
            if (write.value) { memtable->put(write.key, value, sequence, flags); }
            else { memtable->remove(write.key, sequence); }
        }
        if (!recovering) {
            noteTransactionMutation(write.key);
            recordCommittedRaftMutationStats(write.value ? cluster::ReplOpType::PUT : cluster::ReplOpType::REMOVE, flags);
        }
        crashAtTestPoint("cluster_transaction.after_apply_record");
    }
    if (walWriter) { walWriter->forceSync(); }
    if (versionLog) { versionLog->forceSync(); }
    resetCommittedSeq(std::max(snapshotSeq(), sequence));
    if (versionLog) { versionLog->seedCommittedSeq(std::max(snapshotSeq(), sequence)); }
    transactionJournalPending.store(!transactionJournals.empty(), std::memory_order_release);
}
void recoverDistributedState() {
    if (!fs::exists(distributedStatePath())) { return; }
    if (!distributedTransactionsEnabled()) { throw std::invalid_argument("AkkEngine: stored Cluster transaction state requires TWO_PHASE_COMMIT"); }
    std::ifstream input{distributedStatePath(), std::ios::binary};
    const auto size = fs::file_size(distributedStatePath());
    if (size > opts.transactions.maxJournalBytes) { throw std::length_error("Cluster transaction: stored state exceeds journal capacity"); }
    std::vector<uint8_t> bytes(static_cast<size_t>(size));
    if (!input.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(size))) { throw std::runtime_error("Cluster transaction: unreadable state"); }
    input.close();
    distributedState = decodeDistributedState(bytes);
    if (!distributedState.deltaGeneration) { throw std::runtime_error("Cluster transaction: missing prepare journal generation"); }
    distributedCheckpointGeneration = distributedState.deltaGeneration;
    recoverDistributedDeltas(distributedState);
    if (distributedState.decisionGeneration) { distributedResults(true)->activate(distributedState.decisionGeneration); }
    if (distributedState.completionGeneration) { distributedResults(false)->activate(distributedState.completionGeneration); }
    if (distributedReservedBytes(distributedState) > opts.transactions.maxJournalBytes) { throw std::length_error("Cluster transaction: stored preparation exceeds journal capacity"); }
    for (auto& [id, participant] : distributedState.participants) { for (auto& chunk : participant.chunks) { loadDistributedChunk(chunk); } }
    validateDistributedParticipants(distributedState);
    pruneDistributedDeltas(distributedState.deltaGeneration);
    collectDistributedChunks();
    for (auto& [id, participant] : distributedState.participants) {
        if (participant.applyingSequence && !participant.applied) {
            applyDistributedWrites(participant.applyingSequence, participant, true);
            participant.applied = true; distributedState.sequence = std::max(distributedState.sequence, participant.applyingSequence);
            persistDistributedState(distributedState);
        }
    }
    publishRaftAppliedSequence(distributedState.sequence);
}
void applyDistributedCommand(uint64_t sequence, std::span<const uint8_t> bytes) {
    if (!distributedTransactionsEnabled()) { throw std::runtime_error("Cluster transaction: participant policy mismatch"); }
    const auto message = cluster::detail::decodeTransactionMessage(bytes);
    std::unique_lock visibility{transactionVisibilityMu};
    std::lock_guard storage{writeMu};
    std::lock_guard stateLock{distributedStateMu};
    if (sequence <= distributedState.sequence) { return; }
    const bool reservation = message.action == DistributedAction::FREEZE || message.action == DistributedAction::PREPARE ||
        message.action == DistributedAction::LOCK_KEY || message.action == DistributedAction::LOCK_RANGE ||
        message.action == DistributedAction::APPEND || message.action == DistributedAction::SEAL;
    if (reservation && !message.expires) { throw std::runtime_error("Cluster transaction: missing replicated reservation expiry"); }
    // The replicated floor fences expired reservations after receipt cleanup.
    // APPLY is deliberately excluded so decided commits remain recoverable.
    const bool obsolete = reservation && (message.expires <= distributedResults(false)->floor() || distributedCompleted(message.id));
    if (message.action == DistributedAction::APPEND && !obsolete) {
        const auto it = distributedState.participants.find(message.id);
        if (it == distributedState.participants.end()) { throw std::runtime_error("Cluster transaction: unknown replicated participant"); }
        auto& p = it->second;
        if (!p.frozen || p.sealed || message.argument != p.receivedBytes || message.payload.empty() ||
            message.payload.size() > 1024 * 1024 || message.payload.size() > p.expectedBytes - p.receivedBytes) {
            throw std::runtime_error("Cluster transaction: invalid replicated prepare chunk");
        }
        DistributedParticipant::Chunk chunk{sequence, static_cast<uint32_t>(message.payload.size()), distributedChunkDigest(message.payload),
            std::make_shared<const std::vector<uint8_t>>(message.payload)};
        p.chunks.reserve(p.chunks.size() + 1);
        saveDistributedChunk(chunk);
        crashAtTestPoint("cluster_transaction.after_prepare_chunk");
        persistDistributedAppend(p, chunk);
        crashAtTestPoint("cluster_transaction.after_prepare_metadata");
        p.receivedBytes += chunk.size; p.chunks.push_back(std::move(chunk)); distributedState.sequence = sequence;
        distributedStateCv.notify_all();
        publishRaftAppliedSequence(sequence);
        return;
    }
    auto next = distributedState;
    std::optional<DistributedParticipant> released;
    const auto participant = [&]() -> DistributedParticipant& {
        const auto it = next.participants.find(message.id);
        if (it == next.participants.end()) { throw std::runtime_error("Cluster transaction: unknown replicated participant"); }
        return it->second;
    };
    const auto find = [&]() -> DistributedRecord& {
        auto it = next.decisions.find(message.id);
        if (it == next.decisions.end()) { throw std::runtime_error("Cluster transaction: unknown replicated decision"); }
        return it->second;
    };
    if (!obsolete) {
        switch (message.action) {
            case DistributedAction::CREATE: {
                if (!distributedIdPresent(message.id) || !message.expires || next.decisions.contains(message.id)) { throw std::runtime_error("Cluster transaction: invalid replicated creation"); }
                DistributedRecord record; record.expires = message.expires;
                if (!message.payload.empty()) { throw std::runtime_error("Cluster transaction: invalid creation payload"); }
                distributedResults(true)->advanceFloor(message.argument);
                next.decisions.emplace(message.id, std::move(record)); break;
            }
            case DistributedAction::ENLIST: {
                auto& record = find();
                if (record.status != DistributedStatus::OPEN || record.finished) { throw std::runtime_error("Cluster transaction: invalid replicated enlistment"); }
                const auto position = std::ranges::lower_bound(record.participants, message.participant);
                if (position == record.participants.end() || *position != message.participant) { record.participants.insert(position, message.participant); }
                break;
            }
            case DistributedAction::START_PREPARE: {
                auto& record = find();
                if (record.status != DistributedStatus::OPEN && record.status != DistributedStatus::PREPARING) {
                    throw std::runtime_error("Cluster transaction: preparation cannot change a terminal decision");
                }
                record.status = DistributedStatus::PREPARING;
                break;
            }
            case DistributedAction::DECIDE_COMMIT: {
                auto& record = find();
                if (record.status != DistributedStatus::PREPARING && record.status != DistributedStatus::COMMITTED) {
                    throw std::runtime_error("Cluster transaction: commit requires preparation and cannot replace abort");
                }
                record.status = DistributedStatus::COMMITTED; break;
            }
            case DistributedAction::DECIDE_ABORT: {
                auto& record = find();
                if (record.status == DistributedStatus::COMMITTED) { throw std::runtime_error("Cluster transaction: abort cannot replace commit"); }
                record.status = DistributedStatus::ABORTED; break;
            }
            case DistributedAction::READY: {
                auto& record = find();
                if (record.status != DistributedStatus::COMMITTED) { throw std::runtime_error("Cluster transaction: ready requires commit"); }
                record.ready = true; break;
            }
            case DistributedAction::FINISH: {
                auto record = find(); record.finished = true;
                if (record.status != DistributedStatus::ABORTED && !(record.status == DistributedStatus::COMMITTED && record.ready)) {
                    throw std::runtime_error("Cluster transaction: finish requires a publishable terminal decision");
                }
                distributedResults(true)->put(message.id, {record.expires, sequence, encodeDistributedRecord(record)});
                next.decisions.erase(message.id); break;
            }
            case DistributedAction::FREEZE:
            case DistributedAction::PREPARE: {
                if (distributedCompleted(message.id)) { break; }
                if (message.action == DistributedAction::FREEZE) { distributedResults(false)->advanceFloor(message.argument); }
                auto& p = next.participants[message.id];
                p.id = message.id; p.expires = message.expires;
                p.frozen = true;
                if (message.action == DistributedAction::PREPARE) {
                    p.expectedBytes = message.argument;
                    p.blobSequence = sequence;
                    cluster::detail::TransactionReader reader{message.payload}; p.timestamp = reader.integer(); reader.finish();
                }
                break;
            }
            case DistributedAction::LOCK_KEY:
            case DistributedAction::LOCK_RANGE: {
                auto& p = participant();
                if (p.id != message.id || !p.frozen || p.expectedBytes || p.sealed) { throw std::runtime_error("Cluster transaction: invalid replicated lock"); }
                if (message.action == DistributedAction::LOCK_KEY) {
                    if (message.payload.size() > UINT16_MAX) { throw std::runtime_error("Cluster transaction: oversized replicated key lock"); }
                    p.keys.insert(message.payload);
                }
                else {
                    cluster::detail::TransactionReader reader{message.payload}; auto start = reader.field(), end = reader.field(); reader.finish();
                    if (start.size() > UINT16_MAX || end.size() > UINT16_MAX || (!end.empty() && start >= end)) { throw std::runtime_error("Cluster transaction: invalid replicated range lock"); }
                    p.ranges.emplace_back(std::move(start), std::move(end));
                    if (message.argument > 1) { throw std::runtime_error("Cluster transaction: invalid query barrier flag"); }
                    p.queryBarrier = p.queryBarrier || message.argument == 1;
                }
                if (distributedReservationConflict(next, p)) { throw std::runtime_error("Cluster transaction: overlapping replicated reservation"); }
                break;
            }
            case DistributedAction::SEAL: {
                auto& p = participant();
                if (p.receivedBytes != p.expectedBytes) { throw std::runtime_error("Cluster transaction: incomplete replicated preparation"); }
                validateDistributedWrites(cluster::detail::decodeTransactionPrepare(distributedPrepareBytes(p)));
                p.sealed = true; break;
            }
            case DistributedAction::APPLY: {
                auto& p = participant();
                if (p.id != message.id || !p.sealed) { throw std::runtime_error("Cluster transaction: unprepared replicated commit"); }
                if (!p.applied) {
                    p.applyingSequence = sequence;
                    // Redo reaches stable storage before the first public record.
                    persistDistributedState(next);
                    applyDistributedWrites(sequence, p, false);
                    p.applied = true;
                }
                break;
            }
            case DistributedAction::RELEASE:
                // A receipt also fences late reservation proposals after an abort.
                distributedResults(false)->advanceFloor(message.argument);
                distributedResults(false)->put(message.id, {message.expires, sequence, {}});
                if (const auto it = next.participants.find(message.id); it != next.participants.end()) {
                    released = std::move(it->second); next.participants.erase(it);
                }
                break;
            default: throw std::runtime_error("Cluster transaction: invalid replicated action");
        }
    }
    if (distributedDecisions) { distributedDecisions->sync(); }
    if (distributedCompletions) { distributedCompletions->sync(); }
    observeDistributedResults(next);
    next.sequence = sequence; persistDistributedState(next); distributedState = std::move(next);
    if (released) { removeDistributedChunks(*released); }
    distributedStateCv.notify_all();
    publishRaftAppliedSequence(sequence);
}
struct DistributedWriteAdmission {
    std::unique_lock<std::timed_mutex> guard;
    uint64_t term = 0;
};
DistributedWriteAdmission distributedWriteAdmission(std::span<const uint8_t> key) {
    std::unique_lock<std::timed_mutex> guard{distributedProposalMu, std::defer_lock};
    if (!distributedTransactionsEnabled()) { return {std::move(guard), 0}; }
    cluster::detail::ReconfigurationDeadline budget{opts.cluster.runtime.transactions.timeoutMs};
    for (;;) {
        guard = cluster::detail::ReconfigurationDeadline::lock(distributedProposalMu);
        const auto validationTerm = clusterRuntime->transactionReadBarrier();
        {
            std::lock_guard lock{distributedStateMu};
            bool barrier = false;
            for (const auto& [id, p] : distributedState.participants) {
                if (!distributedKeyLocked(p, key)) { continue; }
                if (!p.queryBarrier) {
                    throw cluster::ClusterRoutingError(cluster::ClusterRoutingCode::TRANSACTION_CONFLICT, {}, "Cluster transaction: key or range is reserved; retry write");
                }
                barrier = true;
            }
            if (!barrier) { return {std::move(guard), validationTerm}; }
        }
        // Release proposal admission so the replicated query release can pass.
        guard.unlock();
        std::unique_lock stateLock{distributedStateMu};
        while (std::ranges::any_of(distributedState.participants, [&](const auto& item) {
            return item.second.queryBarrier && distributedKeyLocked(item.second, key);
        })) {
            distributedStateCv.wait_for(stateLock, std::chrono::milliseconds{20});
            stateLock.unlock();
            cluster::detail::ReconfigurationDeadline::check();
            if (placementClosing()) { throw std::runtime_error("Cluster query: engine is closing while write waits"); }
            stateLock.lock();
        }
    }
}
std::unique_lock<std::mutex> distributedReadAdmission(std::span<const uint8_t> start = {}, std::span<const uint8_t> end = {}, bool point = false) const {
    std::unique_lock guard{distributedStateMu, std::defer_lock};
    if (!distributedTransactionsEnabled()) { return guard; }
    guard.lock();
    for (const auto& [id, p] : distributedState.participants) {
        if (!p.sealed && !p.applyingSequence) { continue; }
        const std::pair range{std::vector<uint8_t>{start.begin(), start.end()}, std::vector<uint8_t>{end.begin(), end.end()}};
        const bool intersects = point ? distributedKeyLocked(p, start) :
            std::ranges::any_of(p.keys, [&](const auto& key) { return compareKey(key, start) >= 0 && (end.empty() || compareKey(key, end) < 0); }) ||
            std::ranges::any_of(p.ranges, [&](const auto& reserved) { return distributedRangesOverlap(range, reserved); });
        if (intersects) { throw TransactionConflict("Cluster transaction: commit visibility is pending; retry read"); }
    }
    // Keep application from starting between admission and reading the value.
    return guard;
}

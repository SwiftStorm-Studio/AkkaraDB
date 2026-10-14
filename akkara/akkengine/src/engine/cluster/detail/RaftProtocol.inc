/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */


// Included inside the anonymous namespace in RaftConsensusRuntime.cpp.
enum class RaftRole : uint8_t {
    FOLLOWER, CANDIDATE, LEADER,
};

enum class RaftEntryKind : uint8_t {
    MUTATION = 0, CONFIG_JOINT = 1, CONFIG_FINAL = 2, BLOB = 3, NOOP = 4,
};

struct RequestRecord {
    std::array<uint8_t, 32> fingerprint{};
    uint64_t sequence = 0;
    uint64_t index = 0;
    bool operator==(const RequestRecord&) const = default;
};

struct RequestState {
    uint64_t expiryFloor = 0, generation = 0, sequence = 0, count = 0, bytes = 0;
    bool operator==(const RequestState&) const = default;
};
void writeRequestId(std::vector<uint8_t>& out, const ClusterRequestId& id) {
    out.insert(out.end(), id.nonce.begin(), id.nonce.end()); writeU64(out, id.expiresAtUnixMs);
}
bool readRequestId(std::span<const uint8_t> in, size_t& cursor, ClusterRequestId& id) {
    if (cursor > in.size() || in.size() - cursor < 24) { return false; }
    std::copy_n(in.data() + cursor, 16, id.nonce.begin()); id.expiresAtUnixMs = readU64(in, cursor + 16); cursor += 24;
    return id.expiresAtUnixMs && std::ranges::any_of(id.nonce, [](uint8_t byte) { return byte != 0; });
}
void writeRequestRecord(std::vector<uint8_t>& out, const RequestRecord& record) {
    out.insert(out.end(), record.fingerprint.begin(), record.fingerprint.end()); writeU64(out, record.sequence); writeU64(out, record.index);
}
bool readRequestRecord(std::span<const uint8_t> in, size_t& cursor, RequestRecord& record) {
    if (cursor > in.size() || in.size() - cursor < 48) { return false; }
    std::copy_n(in.data() + cursor, 32, record.fingerprint.begin()); record.sequence = readU64(in, cursor + 32);
    record.index = readU64(in, cursor + 40); cursor += 48; return record.sequence && record.index;
}
std::vector<uint8_t> requestStoreKey(const ClusterRequestId& id) {
    std::vector<uint8_t> key; writeRequestId(key, id); return key;
}
void writeRequestState(std::vector<uint8_t>& out, const RequestState& state) {
    for (auto value : {state.expiryFloor, state.generation, state.sequence, state.count, state.bytes}) { writeU64(out, value); }
}
bool readRequestState(std::span<const uint8_t> in, size_t& cursor, RequestState& state) {
    if (cursor > in.size() || in.size() - cursor < 40) { return false; }
    state = {readU64(in, cursor), readU64(in, cursor + 8), readU64(in, cursor + 16), readU64(in, cursor + 24), readU64(in, cursor + 32)};
    cursor += 40;
    return state.count <= state.bytes / detail::DiskResults::recordCharge(24, 48) &&
        ((state.generation != 0) || (!state.expiryFloor && !state.sequence && !state.count && !state.bytes));
}

struct RaftLogEntry {
    std::optional<ClusterRequestId> requestId;
    std::array<uint8_t, 32> requestFingerprint{};
    uint64_t requestTime = 0;
    uint64_t term = 0;
    uint64_t index = 0;
    // Global state-machine sequence for MUTATION/BLOB entries. Metadata entries keep this at 0.
    uint64_t clientSeq = 0;
    RaftEntryKind kind = RaftEntryKind::MUTATION;
    ReplOpType op = ReplOpType::PUT;
    uint8_t flags = 0;
    bool proposalFinal = true;
    uint64_t sourceNodeId = 0;
    uint64_t timestampNs = 0;
    std::vector<uint8_t> key;
    std::vector<uint8_t> value;
};

struct RequestVote {
    uint64_t term = 0;
    uint64_t candidateId = 0;
    uint64_t lastLogIndex = 0;
    uint64_t lastLogTerm = 0;
};

enum class RaftChannelKind : uint8_t {
    CONSENSUS, FORWARD, PLACEMENT_READ
};

bool isReservedControlForward(const ForwardRequest& request, RaftGroupKind group) {
    if (request.entries.size() != 1 || !request.entries[0].key.empty()) { return false; }
    if (group == RaftGroupKind::REQUEST_AUTHORITY && request.operation == ForwardOperation::CLUSTER_ADMIN) {
        // Receipt commands only await consensus, never a data callback.
        return request.entries[0].value.size() == 90;
    }
    if (request.operation == ForwardOperation::CLUSTER_ADMIN) { return request.entries[0].value.empty(); }
    if (request.operation != ForwardOperation::TRANSACTION) { return false; }
    try { return detail::decodeTransactionMessage(request.entries[0].value).action == detail::TransactionAction::STATUS; }
    catch (const std::exception&) { return false; }
}

struct RaftPeerHello {
    static constexpr uint32_t VERSION = 2;
    uint64_t nodeId = 0;
    ClusterId clusterId{};
    std::array<uint8_t, 32> compatibilityFingerprint{};
    RaftBlobPolicy blobPolicy = RaftBlobPolicy::REJECT;
    bool requestsEnabled = false;
    uint64_t requestMaxRetentionMs = 0;
    uint32_t requestCapacity = 0;
    std::array<uint8_t, 32> groupIdentity{};
    RaftChannelKind channelKind = RaftChannelKind::CONSENSUS;
};

enum class RaftPeerHelloStatus : uint8_t {
    ACCEPTED = 0, UNKNOWN_NODE = 1, CLUSTER_MISMATCH = 2, POLICY_MISMATCH = 3, INVALID = 4,
};

struct RaftPeerHelloResponse {
    RaftPeerHelloStatus status = RaftPeerHelloStatus::INVALID;
};

struct RequestVoteResponse {
    uint64_t term = 0;
    bool voteGranted = false;
};

struct AppendEntries {
    uint64_t term = 0;
    uint64_t leaderId = 0;
    uint64_t prevLogIndex = 0;
    uint64_t prevLogTerm = 0;
    uint64_t leaderCommit = 0;
    std::vector<RaftLogEntry> entries;
};

struct AppendEntriesResponse {
    uint64_t term = 0;
    bool success = false;
    uint64_t matchIndex = 0;
    uint64_t conflictIndex = 0;
    uint64_t conflictTerm = 0;
};

struct SnapshotKvChunk {
    uint64_t entryIndex = 0;
    uint64_t valueOffset = 0;
    uint64_t valueSize = 0;
    uint32_t valueCrc32c = 0;
    std::vector<uint8_t> key;
    std::vector<uint8_t> value;
};

struct InstallSnapshot {
    uint64_t term = 0;
    uint64_t leaderId = 0;
    uint64_t lastIncludedIndex = 0;
    uint64_t lastIncludedTerm = 0;
    uint64_t snapshotSeq = 0;
    uint64_t entryCount = 0;
    uint64_t configurationGeneration = 0;
    std::vector<NodeInfo> committedMembers;
    std::optional<std::vector<NodeInfo>> jointOldMembers;
    std::optional<std::vector<NodeInfo>> jointNewMembers;
    RequestState requestState;
    std::vector<std::pair<ClusterRequestId, RequestRecord>> results;
    bool done = false;
    std::vector<SnapshotKvChunk> chunks;
};

struct InstallSnapshotResponse {
    uint64_t term = 0;
    bool success = false;
    uint64_t lastIncludedIndex = 0;
};

struct TimeoutNow {
    uint64_t term = 0;
    uint64_t leaderId = 0;
    uint64_t targetId = 0;
};

struct TimeoutNowResponse {
    uint64_t term = 0;
    bool accepted = false;
};

struct PeerReplicationState {
    NodeInfo node;
    uint64_t nextIndex = 1;
    uint64_t matchIndex = 0;
    bool inFlight = false;
    uint64_t observedTerm = 0;
    std::optional<Clock::time_point> successfulAppend{};
};

void writeNodeInfo(std::vector<uint8_t>& out, const NodeInfo& node) {
    writeU64(out, node.nodeId);
    writeU32(out, node.capabilities);
    writeU32(out, node.host.size() > UINT32_MAX ? UINT32_MAX : static_cast<uint32_t>(node.host.size()));
    writeU32(out, node.dataPort);
    writeU32(out, node.replPort);
    out.insert(out.end(), node.host.begin(), node.host.end());
    for (const auto label : {std::string_view{node.hostId}, std::string_view{node.siteId}}) {
        writeU32(out, static_cast<uint32_t>(label.size()));
        out.insert(out.end(), label.begin(), label.end());
    }
}

bool readNodeInfo(std::span<const uint8_t> in, size_t& cursor, NodeInfo& node) {
    constexpr size_t FIXED_NODE_INFO_SIZE = 24;
    if (cursor + FIXED_NODE_INFO_SIZE > in.size()) { return false; }
    node.nodeId = readU64(in, cursor);
    cursor += 8;
    node.capabilities = readU32(in, cursor);
    cursor += 4;
    const auto hostLen = readU32(in, cursor);
    cursor += 4;
    node.dataPort = static_cast<uint16_t>(readU32(in, cursor));
    cursor += 4;
    node.replPort = static_cast<uint16_t>(readU32(in, cursor));
    cursor += 4;
    std::vector<uint8_t> host;
    if (!readBytes(in, cursor, hostLen, host)) { return false; }
    node.host.assign(host.begin(), host.end());
    for (auto* label : {&node.hostId, &node.siteId}) {
        if (cursor + 4 > in.size()) { return false; }
        const auto length = readU32(in, cursor);
        cursor += 4;
        if (length > 1024 || cursor + length > in.size()) { return false; }
        label->assign(reinterpret_cast<const char*>(in.data() + cursor), length);
        cursor += length;
    }
    return true;
}

std::vector<uint8_t> encodeNodeSet(const std::vector<NodeInfo>& nodes) {
    std::vector<uint8_t> out;
    writeU32(out, static_cast<uint32_t>(nodes.size()));
    for (const auto& node : nodes) { writeNodeInfo(out, node); }
    return out;
}

bool decodeNodeSet(std::span<const uint8_t> in, std::vector<NodeInfo>& nodes) {
    if (in.size() < 4) { return false; }
    const auto count = readU32(in, 0);
    size_t cursor = 4;
    constexpr size_t MIN_NODE_INFO_SIZE = 24;
    if (count > 512 || count > (in.size() - cursor) / MIN_NODE_INFO_SIZE) { return false; }
    nodes.clear();
    nodes.reserve(count);
    std::unordered_set<uint64_t> ids;
    for (uint32_t i = 0; i < count; ++i) {
        NodeInfo node;
        if (!readNodeInfo(in, cursor, node)) { return false; }
        if (node.nodeId == 0 || !ids.insert(node.nodeId).second || !node.dataBearing() || (node.capabilities & ~(COORDINATOR_ELIGIBLE | DATA_BEARING |
            RAFT_LEARNER)) != 0) { return false; }
        nodes.push_back(std::move(node));
    }
    return cursor == in.size();
}

bool hasVoter(const std::vector<NodeInfo>& members) {
    return std::ranges::any_of(members, [](const NodeInfo& node) { return node.dataBearing() && !node.raftLearner(); });
}

void writeNodeSetField(std::vector<uint8_t>& out, const std::vector<NodeInfo>& nodes) {
    const auto encoded = encodeNodeSet(nodes);
    writeU32(out, static_cast<uint32_t>(encoded.size()));
    out.insert(out.end(), encoded.begin(), encoded.end());
}

bool readNodeSetField(std::span<const uint8_t> in, size_t& cursor, std::vector<NodeInfo>& nodes) {
    if (cursor + 4 > in.size()) { return false; }
    const auto len = readU32(in, cursor);
    cursor += 4;
    if (cursor + len > in.size()) { return false; }
    const auto encoded = in.subspan(cursor, len);
    cursor += len;
    return decodeNodeSet(encoded, nodes);
}

void writeOptionalNodeSetField(std::vector<uint8_t>& out, const std::optional<std::vector<NodeInfo>>& nodes) {
    writeU8(out, nodes ? 1 : 0);
    if (nodes) { writeNodeSetField(out, *nodes); }
}

bool readOptionalNodeSetField(std::span<const uint8_t> in, size_t& cursor, std::optional<std::vector<NodeInfo>>& nodes) {
    if (cursor + 1 > in.size()) { return false; }
    const auto hasValue = in[cursor++] != 0;
    if (!hasValue) {
        nodes.reset();
        return true;
    }
    std::vector<NodeInfo> decoded;
    if (!readNodeSetField(in, cursor, decoded)) { return false; }
    nodes = std::move(decoded);
    return true;
}

std::vector<uint8_t> encodeEntryPayload(const RaftLogEntry& entry) {
    std::vector<uint8_t> out;
    writeU64(out, entry.term);
    writeU64(out, entry.index);
    writeU64(out, entry.clientSeq);
    writeU8(out, std::to_underlying(entry.kind));
    writeU8(out, std::to_underlying(entry.op));
    writeU8(out, entry.flags);
    writeU8(out, entry.proposalFinal ? 1 : 0);
    writeU64(out, entry.sourceNodeId);
    writeU64(out, entry.timestampNs);
    writeU32(out, static_cast<uint32_t>(entry.key.size()));
    writeU32(out, static_cast<uint32_t>(entry.value.size()));
    writeU8(out, entry.requestId ? 1 : 0);
    if (entry.requestId) {
        writeRequestId(out, *entry.requestId);
        out.insert(out.end(), entry.requestFingerprint.begin(), entry.requestFingerprint.end());
        writeU64(out, entry.requestTime);
    }
    out.insert(out.end(), entry.key.begin(), entry.key.end());
    out.insert(out.end(), entry.value.begin(), entry.value.end());
    return out;
}

bool decodeEntryPayload(std::span<const uint8_t> in, size_t& cursor, RaftLogEntry& out) {
    if (cursor + 53 > in.size()) { return false; }
    out.term = readU64(in, cursor);
    cursor += 8;
    out.index = readU64(in, cursor);
    cursor += 8;
    out.clientSeq = readU64(in, cursor);
    cursor += 8;
    out.kind = static_cast<RaftEntryKind>(in[cursor++]);
    out.op = static_cast<ReplOpType>(in[cursor++]);
    out.flags = in[cursor++];
    const auto proposalFinal = in[cursor++];
    if (proposalFinal > 1) { return false; }
    out.proposalFinal = proposalFinal != 0;
    out.sourceNodeId = readU64(in, cursor);
    cursor += 8;
    out.timestampNs = readU64(in, cursor);
    cursor += 8;
    const auto keyLen = readU32(in, cursor);
    cursor += 4;
    const auto valueLen = readU32(in, cursor);
    cursor += 4;
    const auto hasRequest = in[cursor++];
    if (hasRequest > 1) { return false; }
    out.requestId.reset();
    if (hasRequest) {
        ClusterRequestId id;
        if (!readRequestId(in, cursor, id) || in.size() - cursor < 40) { return false; }
        out.requestId = id;
        std::copy_n(in.data() + cursor, 32, out.requestFingerprint.begin());
        out.requestTime = readU64(in, cursor + 32);
        cursor += 40;
    }
    return readBytes(in, cursor, keyLen, out.key) && readBytes(in, cursor, valueLen, out.value);
}

std::vector<uint8_t> encodeRaftFrame(ReplMsgType type, std::span<const uint8_t> payload) {
    if (payload.size() > ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE) { return {}; }
    return encodeFrame(type, payload);
}

std::vector<uint8_t> encodeRequestVote(const RequestVote& rpc, ReplMsgType type = ReplMsgType::RAFT_REQUEST_VOTE) {
    std::vector<uint8_t> out;
    writeU64(out, rpc.term);
    writeU64(out, rpc.candidateId);
    writeU64(out, rpc.lastLogIndex);
    writeU64(out, rpc.lastLogTerm);
    return encodeRaftFrame(type, out);
}

std::vector<uint8_t> encodeRaftPeerHello(const RaftPeerHello& hello) {
    std::vector<uint8_t> out;
    writeU32(out, RaftPeerHello::VERSION);
    writeU64(out, hello.nodeId);
    out.insert(out.end(), hello.clusterId.begin(), hello.clusterId.end());
    out.insert(out.end(), hello.compatibilityFingerprint.begin(), hello.compatibilityFingerprint.end());
    writeU8(out, std::to_underlying(hello.blobPolicy));
    writeU8(out, hello.requestsEnabled ? 1 : 0);
    writeU64(out, hello.requestMaxRetentionMs);
    writeU32(out, hello.requestCapacity);
    out.insert(out.end(), hello.groupIdentity.begin(), hello.groupIdentity.end());
    writeU8(out, std::to_underlying(hello.channelKind));
    return encodeRaftFrame(ReplMsgType::RAFT_PEER_HELLO, out);
}

bool decodeRaftPeerHello(std::span<const uint8_t> in, RaftPeerHello& out) {
    constexpr size_t EXPECTED_SIZE = 107;
    if (in.size() != EXPECTED_SIZE || readU32(in, 0) != RaftPeerHello::VERSION || in[60] > std::to_underlying(RaftBlobPolicy::RAFT_LOG) || in[61] > 1
        || in[106] > std::to_underlying(RaftChannelKind::PLACEMENT_READ)) { return false; }
    out.nodeId = readU64(in, 4);
    std::copy_n(in.data() + 12, out.clusterId.size(), out.clusterId.begin());
    std::copy_n(in.data() + 28, out.compatibilityFingerprint.size(), out.compatibilityFingerprint.begin());
    out.blobPolicy = static_cast<RaftBlobPolicy>(in[60]);
    out.requestsEnabled = in[61] != 0;
    out.requestMaxRetentionMs = readU64(in, 62);
    out.requestCapacity = readU32(in, 70);
    std::copy_n(in.data() + 74, out.groupIdentity.size(), out.groupIdentity.begin());
    out.channelKind = static_cast<RaftChannelKind>(in[106]);
    return out.nodeId != 0;
}

std::vector<uint8_t> encodeRaftPeerHelloResponse(const RaftPeerHelloResponse& response) {
    const std::array<uint8_t, 1> payload{std::to_underlying(response.status)};
    return encodeRaftFrame(ReplMsgType::RAFT_PEER_HELLO_RESPONSE, payload);
}

bool decodeRaftPeerHelloResponse(std::span<const uint8_t> in, RaftPeerHelloResponse& out) {
    if (in.size() != 1 || in[0] > std::to_underlying(RaftPeerHelloStatus::INVALID)) { return false; }
    out.status = static_cast<RaftPeerHelloStatus>(in[0]);
    return true;
}

bool hasRaftBlobStorage(const ClusterEngineCallbacks& callbacks) {
    return callbacks.beginBlob && callbacks.appendBlobChunk && callbacks.finishBlob;
}

void writeDataApplyContract(std::vector<uint8_t>& bytes, const ClusterRuntimeOptions& options,
    const ClusterEngineCallbacks& callbacks) {
    if (options.raftGroupKind != RaftGroupKind::DATA && options.raftGroupKind != RaftGroupKind::PARTITION_DATA) { return; }
    bytes.insert(bytes.end(), {'D', 'A', 'T', 'A', '-', 'R', 'E', 'C', 'O', 'R', 'D', '1'});
    bytes.insert(bytes.end(), {'W', 'A', 'L', '-', 'B', 'O', 'U', 'N', 'D', 'A', 'R', 'Y', '2'});
    if (options.raftBlobPolicy == RaftBlobPolicy::RAFT_LOG) { writeU8(bytes, hasRaftBlobStorage(callbacks) ? 1 : 0); }
}

std::array<uint8_t, 32> raftCompatibilityFingerprint(
    const ClusterConfig& config,
    const ClusterRuntimeOptions& runtimeOptions,
    const ClusterEngineCallbacks& callbacks
) {
    std::vector<uint8_t> bytes{'A', 'K', 'R', 'P', '3'};
    bytes.insert(bytes.end(), {'F', 'A', 'U', 'L', 'T', 'D', 'O', 'M', 'A', 'I', 'N', 'S'});
    writeU32(bytes, config.faultDomains().minHosts);
    writeU32(bytes, config.faultDomains().minSites);
    writeU8(bytes, config.faultDomains().requireSiteQuorum);
    const auto consistency = config.consistency();
    const auto raft = config.raft();
    writeU8(bytes, std::to_underlying(config.mode()));
    writeU8(bytes, std::to_underlying(consistency.mode));
    writeU8(bytes, std::to_underlying(config.failover()));
    writeU8(bytes, std::to_underlying(consistency.writeConsistency));
    writeU8(bytes, std::to_underlying(consistency.ackTimeoutAction));
    writeU8(bytes, std::to_underlying(consistency.replicaLagAction));
    writeU8(bytes, std::to_underlying(raft.membership.mode));
    writeU8(bytes, raft.membership.allowOnlineVoterChanges ? 1 : 0);
    writeU8(bytes, raft.membership.allowLearners ? 1 : 0);
    writeU8(bytes, std::to_underlying(runtimeOptions.raftBlobPolicy));
    writeU8(bytes, runtimeOptions.raftSnapshot.enabled ? 1 : 0);
    writeU8(bytes, std::to_underlying(runtimeOptions.transactions.mode));
    writeU64(bytes, runtimeOptions.requests.maxRetainedResultBytes);
    writeDataApplyContract(bytes, runtimeOptions, callbacks);
    if (runtimeOptions.raftGroupKind == RaftGroupKind::PLACEMENT) {
        bytes.insert(bytes.end(), {'P', 'L', 'A', 'C', 'E', 'M', 'E', 'N', 'T', '-', 'S', 'T', 'A', 'T', 'E', '3'});
    }
    if (runtimeOptions.raftGroupKind == RaftGroupKind::PLACEMENT && callbacks.reclaimStripeGeneration) {
        bytes.insert(bytes.end(), {'G', 'C', '-', 'A', 'U', 'T', 'H', 'O', 'R', 'I', 'T', 'Y', '1'});
        bytes.insert(bytes.end(), {'H', 'I', 'S', 'T', 'O', 'R', 'Y', '-', 'U', 'N', 'I', 'O', 'N', '1'});
        bytes.insert(bytes.end(), {'S', 'T', 'O', 'R', 'A', 'G', 'E', '-', 'R', 'E', 'C', 'O', 'R', 'D', '1'});
    }
    if (runtimeOptions.transactions.mode != ClusterTransactionMode::DISABLED) {
        bytes.insert(bytes.end(), {'E', 'N', 'L', 'I', 'S', 'T'});
        bytes.insert(bytes.end(), {'K', 'E', 'Y', 'R', 'A', 'N', 'G', 'E'});
        bytes.insert(bytes.end(), {'Q', 'B', 'A', 'R', 'R', 'I', 'E', 'R', '3'});
        bytes.insert(bytes.end(), {'M', 'U', 'L', 'T', 'I', 'P', 'R', 'E', 'P', 'A', 'R', 'E', '2'});
        bytes.insert(bytes.end(), {'S', 'T', 'O', 'R', 'A', 'G', 'E', '-', 'R', 'E', 'C', 'O', 'R', 'D', '1'});
        writeU64(bytes, callbacks.transactionMaxJournalBytes);
        writeU64(bytes, callbacks.transactionBlobThresholdBytes);
        writeU64(bytes, runtimeOptions.transactions.maxRetainedResultBytes);
    }
    const std::array parts{std::span<const uint8_t>{bytes}};
    return crypto::hash256(parts);
}

std::array<uint8_t, 32> raftTransactionApplyFingerprint(const ClusterRuntimeOptions& options, const ClusterEngineCallbacks& callbacks) {
    std::vector<uint8_t> bytes{'A', 'K', 'T', 'A', '3', std::to_underlying(options.transactions.mode)};
    writeU8(bytes, options.requests.enabled ? 1 : 0);
    writeU64(bytes, options.requests.maxRetentionMs);
    writeU32(bytes, options.requests.maxPendingRequests);
    writeU64(bytes, options.requests.maxRetainedResultBytes);
    writeDataApplyContract(bytes, options, callbacks);
    if (options.raftGroupKind == RaftGroupKind::PLACEMENT) {
        bytes.insert(bytes.end(), {'P', 'L', 'A', 'C', 'E', 'M', 'E', 'N', 'T', '-', 'S', 'T', 'A', 'T', 'E', '3'});
    }
    if (options.transactions.mode != ClusterTransactionMode::DISABLED) {
        bytes.insert(bytes.end(), {'E', 'N', 'L', 'I', 'S', 'T'});
        bytes.insert(bytes.end(), {'K', 'E', 'Y', 'R', 'A', 'N', 'G', 'E'});
        bytes.insert(bytes.end(), {'Q', 'B', 'A', 'R', 'R', 'I', 'E', 'R', '3'});
        bytes.insert(bytes.end(), {'M', 'U', 'L', 'T', 'I', 'P', 'R', 'E', 'P', 'A', 'R', 'E', '2'});
        bytes.insert(bytes.end(), {'S', 'T', 'O', 'R', 'A', 'G', 'E', '-', 'R', 'E', 'C', 'O', 'R', 'D', '1'});
        writeU64(bytes, callbacks.transactionMaxJournalBytes);
        writeU64(bytes, callbacks.transactionBlobThresholdBytes);
        writeU64(bytes, options.transactions.maxRetainedResultBytes);
    }
    const std::array parts{std::span<const uint8_t>{bytes}};
    return crypto::hash256(parts);
}

std::array<uint8_t, 32> raftGroupIdentity(const ClusterConfig& config, const ClusterRuntimeOptions& options) {
    std::vector<uint8_t> bytes{'A', 'K', 'R', 'G', '1'};
    bytes.insert(bytes.end(), config.clusterId().begin(), config.clusterId().end());
    writeU64(bytes, options.clusterGroupId);
    writeU8(bytes, std::to_underlying(options.raftGroupKind));
    writeU32(bytes, options.raftPartitionIndex);
    const std::array parts{std::span<const uint8_t>{bytes}};
    return crypto::hash256(parts);
}

bool decodeRequestVote(std::span<const uint8_t> in, RequestVote& out) {
    if (in.size() != 32) { return false; }
    out.term = readU64(in, 0);
    out.candidateId = readU64(in, 8);
    out.lastLogIndex = readU64(in, 16);
    out.lastLogTerm = readU64(in, 24);
    return true;
}

std::vector<uint8_t> encodeRequestVoteResponse(const RequestVoteResponse& rpc, ReplMsgType type = ReplMsgType::RAFT_REQUEST_VOTE_RESPONSE) {
    std::vector<uint8_t> out;
    writeU64(out, rpc.term);
    writeU8(out, rpc.voteGranted ? 1 : 0);
    return encodeRaftFrame(type, out);
}

bool decodeRequestVoteResponse(std::span<const uint8_t> in, RequestVoteResponse& out) {
    if (in.size() != 9 || in[8] > 1) { return false; }
    out.term = readU64(in, 0);
    out.voteGranted = in[8] != 0;
    return true;
}

std::vector<uint8_t> encodeAppendEntries(const AppendEntries& rpc) {
    std::vector<uint8_t> out;
    writeU64(out, rpc.term);
    writeU64(out, rpc.leaderId);
    writeU64(out, rpc.prevLogIndex);
    writeU64(out, rpc.prevLogTerm);
    writeU64(out, rpc.leaderCommit);
    writeU32(out, static_cast<uint32_t>(rpc.entries.size()));
    for (const auto& entry : rpc.entries) {
        const auto payload = encodeEntryPayload(entry);
        writeU32(out, static_cast<uint32_t>(payload.size()));
        out.insert(out.end(), payload.begin(), payload.end());
    }
    return encodeRaftFrame(ReplMsgType::RAFT_APPEND_ENTRIES, out);
}

bool decodeAppendEntries(std::span<const uint8_t> in, AppendEntries& out) {
    if (in.size() < 44) { return false; }
    out.term = readU64(in, 0);
    out.leaderId = readU64(in, 8);
    out.prevLogIndex = readU64(in, 16);
    out.prevLogTerm = readU64(in, 24);
    out.leaderCommit = readU64(in, 32);
    const auto count = readU32(in, 40);
    size_t cursor = 44;
    if (count > (in.size() - cursor) / 4) { return false; }
    out.entries.clear();
    out.entries.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        if (cursor + 4 > in.size()) { return false; }
        const auto entryLen = readU32(in, cursor);
        cursor += 4;
        if (cursor + entryLen > in.size()) { return false; }
        size_t entryCursor = 0;
        RaftLogEntry entry;
        if (!decodeEntryPayload(in.subspan(cursor, entryLen), entryCursor, entry) || entryCursor != entryLen) { return false; }
        cursor += entryLen;
        out.entries.push_back(std::move(entry));
    }
    return cursor == in.size();
}

std::vector<uint8_t> encodeAppendEntriesResponse(const AppendEntriesResponse& rpc) {
    std::vector<uint8_t> out;
    writeU64(out, rpc.term);
    writeU8(out, rpc.success ? 1 : 0);
    writeU64(out, rpc.matchIndex);
    writeU64(out, rpc.conflictIndex);
    writeU64(out, rpc.conflictTerm);
    return encodeRaftFrame(ReplMsgType::RAFT_APPEND_ENTRIES_RESPONSE, out);
}

bool decodeAppendEntriesResponse(std::span<const uint8_t> in, AppendEntriesResponse& out) {
    if (in.size() != 33) { return false; }
    out.term = readU64(in, 0);
    out.success = in[8] != 0;
    out.matchIndex = readU64(in, 9);
    out.conflictIndex = readU64(in, 17);
    out.conflictTerm = readU64(in, 25);
    return true;
}

std::vector<uint8_t> encodeInstallSnapshot(const InstallSnapshot& rpc) {
    std::vector<uint8_t> out;
    writeU64(out, rpc.term);
    writeU64(out, rpc.leaderId);
    writeU64(out, rpc.lastIncludedIndex);
    writeU64(out, rpc.lastIncludedTerm);
    writeU64(out, rpc.snapshotSeq);
    writeU64(out, rpc.entryCount);
    writeU64(out, rpc.configurationGeneration);
    writeNodeSetField(out, rpc.committedMembers);
    writeOptionalNodeSetField(out, rpc.jointOldMembers);
    writeOptionalNodeSetField(out, rpc.jointNewMembers);
    writeRequestState(out, rpc.requestState);
    writeU32(out, static_cast<uint32_t>(rpc.results.size()));
    for (const auto& [id, record] : rpc.results) { writeRequestId(out, id); writeRequestRecord(out, record); }
    writeU8(out, rpc.done ? 1 : 0);
    writeU32(out, static_cast<uint32_t>(rpc.chunks.size()));
    for (const auto& chunk : rpc.chunks) {
        writeU64(out, chunk.entryIndex);
        writeU64(out, chunk.valueOffset);
        writeU64(out, chunk.valueSize);
        writeU32(out, chunk.valueCrc32c);
        writeU32(out, static_cast<uint32_t>(chunk.key.size()));
        writeU32(out, static_cast<uint32_t>(chunk.value.size()));
        out.insert(out.end(), chunk.key.begin(), chunk.key.end());
        out.insert(out.end(), chunk.value.begin(), chunk.value.end());
    }
    return encodeRaftFrame(ReplMsgType::RAFT_INSTALL_SNAPSHOT, out);
}

bool decodeInstallSnapshot(std::span<const uint8_t> in, InstallSnapshot& out) {
    if (in.size() < 61) { return false; }
    out.term = readU64(in, 0);
    out.leaderId = readU64(in, 8);
    out.lastIncludedIndex = readU64(in, 16);
    out.lastIncludedTerm = readU64(in, 24);
    out.snapshotSeq = readU64(in, 32);
    out.entryCount = readU64(in, 40);
    out.configurationGeneration = readU64(in, 48);
    size_t cursor = 56;
    if (!readNodeSetField(in, cursor, out.committedMembers) || !readOptionalNodeSetField(in, cursor, out.jointOldMembers) || !readOptionalNodeSetField(
        in,
        cursor,
        out.jointNewMembers
    )) { return false; }
    if (!readRequestState(in, cursor, out.requestState) || in.size() - cursor < 4) { return false; }
    const auto resultCount = readU32(in, cursor); cursor += 4;
    if (resultCount > (in.size() - cursor) / 72) { return false; }
    out.results.clear(); out.results.reserve(resultCount);
    for (uint32_t i = 0; i < resultCount; ++i) {
        ClusterRequestId id; RequestRecord record;
        if (!readRequestId(in, cursor, id) || !readRequestRecord(in, cursor, record) || id.expiresAtUnixMs <= out.requestState.expiryFloor ||
            record.index > out.lastIncludedIndex || record.sequence > out.snapshotSeq) { return false; }
        out.results.emplace_back(id, record);
    }
    if (in.size() - cursor < 5) { return false; }
    out.done = in[cursor++] != 0;
    if (!out.done && (out.requestState.count || out.requestState.bytes || out.requestState.sequence)) { return false; }
    const auto count = readU32(in, cursor);
    cursor += 4;
    if (count > (in.size() - cursor) / 36) { return false; }
    out.chunks.clear();
    out.chunks.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        if (cursor + 36 > in.size()) { return false; }
        SnapshotKvChunk chunk;
        chunk.entryIndex = readU64(in, cursor);
        cursor += 8;
        chunk.valueOffset = readU64(in, cursor);
        cursor += 8;
        chunk.valueSize = readU64(in, cursor);
        cursor += 8;
        chunk.valueCrc32c = readU32(in, cursor);
        cursor += 4;
        const auto keyLen = readU32(in, cursor);
        cursor += 4;
        const auto valueLen = readU32(in, cursor);
        cursor += 4;
        if (chunk.valueOffset > chunk.valueSize || valueLen > chunk.valueSize - chunk.valueOffset) { return false; }
        if (!readBytes(in, cursor, keyLen, chunk.key) || !readBytes(in, cursor, valueLen, chunk.value)) { return false; }
        out.chunks.push_back(std::move(chunk));
    }
    return cursor == in.size();
}

std::vector<uint8_t> encodeInstallSnapshotResponse(const InstallSnapshotResponse& rpc) {
    std::vector<uint8_t> out;
    writeU64(out, rpc.term);
    writeU8(out, rpc.success ? 1 : 0);
    writeU64(out, rpc.lastIncludedIndex);
    return encodeRaftFrame(ReplMsgType::RAFT_INSTALL_SNAPSHOT_RESPONSE, out);
}

bool decodeInstallSnapshotResponse(std::span<const uint8_t> in, InstallSnapshotResponse& out) {
    if (in.size() != 17) { return false; }
    out.term = readU64(in, 0);
    out.success = in[8] != 0;
    out.lastIncludedIndex = readU64(in, 9);
    return true;
}

std::vector<uint8_t> encodeTimeoutNow(const TimeoutNow& rpc) {
    std::vector<uint8_t> out;
    writeU64(out, rpc.term);
    writeU64(out, rpc.leaderId);
    writeU64(out, rpc.targetId);
    return encodeRaftFrame(ReplMsgType::RAFT_TIMEOUT_NOW, out);
}

bool decodeTimeoutNow(std::span<const uint8_t> in, TimeoutNow& out) {
    if (in.size() != 24) { return false; }
    out.term = readU64(in, 0);
    out.leaderId = readU64(in, 8);
    out.targetId = readU64(in, 16);
    return true;
}

std::vector<uint8_t> encodeTimeoutNowResponse(const TimeoutNowResponse& rpc) {
    std::vector<uint8_t> out;
    writeU64(out, rpc.term);
    writeU8(out, rpc.accepted ? 1 : 0);
    return encodeRaftFrame(ReplMsgType::RAFT_TIMEOUT_NOW_RESPONSE, out);
}

bool decodeTimeoutNowResponse(std::span<const uint8_t> in, TimeoutNowResponse& out) {
    if (in.size() != 9) { return false; }
    out.term = readU64(in, 0);
    out.accepted = in[8] != 0;
    return true;
}

bool sameEntry(const RaftLogEntry& lhs, const RaftLogEntry& rhs) {
    return lhs.term == rhs.term && lhs.index == rhs.index && lhs.clientSeq == rhs.clientSeq && lhs.kind == rhs.kind && lhs.op == rhs.op && lhs.flags ==
        rhs.flags && lhs.proposalFinal == rhs.proposalFinal && lhs.sourceNodeId == rhs.sourceNodeId && lhs.timestampNs == rhs.timestampNs && lhs.key ==
        rhs.key && lhs.value == rhs.value && lhs.requestId == rhs.requestId && lhs.requestFingerprint == rhs.requestFingerprint && lhs.requestTime ==
        rhs.requestTime;
}

bool isStateMachineEntry(RaftEntryKind kind) noexcept { return kind == RaftEntryKind::MUTATION || kind == RaftEntryKind::BLOB; }

bool isKnownRaftEntryKind(RaftEntryKind kind) noexcept {
    return kind == RaftEntryKind::MUTATION || kind == RaftEntryKind::CONFIG_JOINT || kind == RaftEntryKind::CONFIG_FINAL || kind == RaftEntryKind::BLOB
        || kind == RaftEntryKind::NOOP;
}

bool isValidRecordFlags(uint8_t flags) noexcept {
    constexpr uint8_t validFlags = core::MemHdr16::FLAG_TOMBSTONE | core::MemHdr16::FLAG_BLOB;
    return (flags & ~validFlags) == 0;
}

bool advanceStateMachineSequenceTracker(const RaftLogEntry& entry, uint64_t& lastSeq, bool& seqHasMutation) noexcept {
    if (!isStateMachineEntry(entry.kind)) { return entry.clientSeq == 0; }
    if (entry.clientSeq == 0 || entry.clientSeq < lastSeq) { return false; }
    if (entry.clientSeq != lastSeq) {
        lastSeq = entry.clientSeq;
        seqHasMutation = false;
    }
    if (entry.kind == RaftEntryKind::MUTATION) {
        if (seqHasMutation) { return false; }
        seqHasMutation = true;
    }
    return true;
}

struct RaftBlobChunk {
    uint64_t blobId = 0;
    uint64_t offset = 0;
    uint64_t totalSize = 0;
    uint32_t contentCrc32c = 0;
};

static constexpr size_t RAFT_APPEND_ENTRIES_BASE_SIZE = 44;
static constexpr size_t RAFT_ENTRY_LENGTH_PREFIX_SIZE = 4;
static constexpr size_t RAFT_ENTRY_PAYLOAD_BASE_SIZE = 53;
static constexpr size_t RAFT_BLOB_CHUNK_KEY_SIZE = 28;

uint32_t maxRaftBlobChunkSizeBytes() {
    return ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE - static_cast<uint32_t>(RAFT_APPEND_ENTRIES_BASE_SIZE + RAFT_ENTRY_LENGTH_PREFIX_SIZE +
        RAFT_ENTRY_PAYLOAD_BASE_SIZE + RAFT_BLOB_CHUNK_KEY_SIZE);
}

std::vector<uint8_t> encodeBlobChunkKey(const RaftBlobChunk& chunk) {
    std::vector<uint8_t> out;
    writeU64(out, chunk.blobId);
    writeU64(out, chunk.offset);
    writeU64(out, chunk.totalSize);
    writeU32(out, chunk.contentCrc32c);
    return out;
}

bool decodeBlobChunkKey(std::span<const uint8_t> in, RaftBlobChunk& chunk) {
    if (in.size() != RAFT_BLOB_CHUNK_KEY_SIZE) { return false; }
    chunk.blobId = readU64(in, 0);
    chunk.offset = readU64(in, 8);
    chunk.totalSize = readU64(in, 16);
    chunk.contentCrc32c = readU32(in, 24);
    if (chunk.offset > chunk.totalSize) { return false; }
    return true;
}

bool isValidRaftLogEntryShape(const RaftLogEntry& entry) {
    if (!isKnownRaftEntryKind(entry.kind)) { return false; }
    if (entry.requestId && (entry.kind != RaftEntryKind::MUTATION || entry.requestTime == 0 || entry.requestTime >= entry.requestId->expiresAtUnixMs)) {
        return false;
    }
    constexpr size_t maxEntryPayload = ReplFrameHeader::MAX_RAFT_PAYLOAD_SIZE - RAFT_APPEND_ENTRIES_BASE_SIZE - RAFT_ENTRY_LENGTH_PREFIX_SIZE;
    const size_t entryMetadata = RAFT_ENTRY_PAYLOAD_BASE_SIZE + (entry.requestId ? 64u : 0u);
    if (entryMetadata > maxEntryPayload || entry.key.size() > maxEntryPayload - entryMetadata || entry.value.size() > maxEntryPayload - entryMetadata -
        entry.key.size()) { return false; }
    if (entry.kind == RaftEntryKind::BLOB) {
        RaftBlobChunk chunk;
        return entry.op == ReplOpType::PUT && entry.flags == 0 && decodeBlobChunkKey(entry.key, chunk) && entry.value.size() <= chunk.totalSize - chunk.
            offset;
    }
    if (entry.kind == RaftEntryKind::CONFIG_JOINT) {
        std::vector<NodeInfo> oldMembers;
        std::vector<NodeInfo> newMembers;
        return entry.clientSeq == 0 && entry.op == ReplOpType::PUT && entry.flags == 0 && decodeNodeSet(entry.key, oldMembers) && decodeNodeSet(
            entry.value,
            newMembers
        ) && hasVoter(oldMembers) && hasVoter(newMembers);
    }
    if (entry.kind == RaftEntryKind::CONFIG_FINAL) {
        std::vector<NodeInfo> oldMembers;
        std::vector<NodeInfo> newMembers;
        return entry.clientSeq == 0 && entry.op == ReplOpType::PUT && entry.flags == 0 && (entry.key.empty() || decodeNodeSet(entry.key, oldMembers)) &&
            decodeNodeSet(entry.value, newMembers) && hasVoter(newMembers) && (entry.key.empty() || hasVoter(oldMembers));
    }
    if (entry.kind == RaftEntryKind::NOOP) {
        return entry.clientSeq == 0 && entry.op == ReplOpType::PUT && entry.flags == 0 && entry.key.empty() && entry.value.empty();
    }
    return (entry.op == ReplOpType::PUT || entry.op == ReplOpType::REMOVE || (entry.op == ReplOpType::TRANSACTION && entry.flags == 0)) &&
        isValidRecordFlags(entry.flags);
}

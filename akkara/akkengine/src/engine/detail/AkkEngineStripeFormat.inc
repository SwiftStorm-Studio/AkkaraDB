/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/AkkEngineStripeFormat.inc
// Included inside AkkEngine::Impl by AkkEngineImpl.hpp.
struct StripeMetadata {
    bool tombstone = false;
    bool rollback = false;
    uint64_t version = 0;
    uint64_t ownerNodeId = 0;
    uint64_t authorityNodeId = 0;
    uint64_t fenceToken = 0;
    uint64_t originalSize = 0;
    erasure::ErasureLayout layout;
    uint8_t copiesPerShard = 1;
    std::vector<uint64_t> nodeIds;
    std::vector<uint8_t> shardPresent;
    uint64_t logicalSeq = 0;
    uint64_t originNodeId = 0;
    uint64_t timestampNs = 0;
};

static void pushU16(std::vector<uint8_t>& out, uint16_t value) {
    out.push_back(value);
    out.push_back(value >> 8);
}

static void pushU32(std::vector<uint8_t>& out, uint32_t value) {
    for (size_t i = 0; i < 4; ++i) { out.push_back(value >> (i * 8)); }
}

static void pushU64(std::vector<uint8_t>& out, uint64_t value) {
    for (size_t i = 0; i < 8; ++i) { out.push_back(value >> (i * 8)); }
}

static bool pullU16(std::span<const uint8_t> bytes, size_t& cursor, uint16_t& out) {
    if (cursor + 2 > bytes.size()) { return false; }
    out = bytes[cursor] | bytes[cursor + 1] << 8;
    cursor += 2;
    return true;
}

static bool pullU32(std::span<const uint8_t> bytes, size_t& cursor, uint32_t& out) {
    if (cursor + 4 > bytes.size()) { return false; }
    out = 0;
    for (size_t i = 0; i < 4; ++i) { out |= static_cast<uint32_t>(bytes[cursor + i]) << (i * 8); }
    cursor += 4;
    return true;
}

static bool pullU64(std::span<const uint8_t> bytes, size_t& cursor, uint64_t& out) {
    if (cursor + 8 > bytes.size()) { return false; }
    out = 0;
    for (size_t i = 0; i < 8; ++i) { out |= static_cast<uint64_t>(bytes[cursor + i]) << (i * 8); }
    cursor += 8;
    return true;
}

static bool pullBytes(std::span<const uint8_t> bytes, size_t& cursor, size_t size, std::vector<uint8_t>& out) {
    if (size > bytes.size() - cursor) { return false; }
    out.assign_range(bytes.subspan(cursor, size));
    cursor += size;
    return true;
}

static std::vector<uint8_t> stripeMetaKey(std::span<const uint8_t> key) {
    // A fixed namespace followed by the raw key preserves public
    // byte order and permits bounded metadata range seeks.
    std::vector<uint8_t> out{0, 'A', 'K', 'S', 'M', '1'};
    out.append_range(key);
    return out;
}

static std::vector<uint8_t> stripeHistoryPlacementKey(std::span<const uint8_t> key, uint64_t sequence, uint64_t generation) {
    std::vector<uint8_t> out{0, 'A', 'K', 'S', 'H', '2'};
    pushU64(out, generation);
    pushU64(out, sequence);
    out.append_range(key);
    return out;
}

StripeMetadata effectiveStripeMetadata(std::span<const uint8_t> key, const StripeMetadata& original) {
    const auto active = activePlacementGeneration.load(std::memory_order_acquire);
    const auto value = getValueInternal(stripeHistoryPlacementKey(key, original.logicalSeq, active), false);
    if (!value) { return original; }
    size_t cursor = 0;
    uint64_t generation = 0;
    if (!pullU64(*value, cursor, generation)) { throw std::runtime_error("AkkEngine: malformed stripe placement alias"); }
    if (generation != active) { throw std::runtime_error("AkkEngine: stripe alias placement generation mismatch"); }
    auto metadata = decodeStripeMetadata(std::span{*value}.subspan(cursor));
    if (!metadata || metadata->logicalSeq != original.logicalSeq || metadata->version != original.version || metadata->originNodeId != original.
        originNodeId || metadata->timestampNs != original.timestampNs) { throw std::runtime_error("AkkEngine: invalid stripe placement alias"); }
    return *metadata;
}

static std::vector<uint8_t> stripeShardKey(std::span<const uint8_t> key, uint64_t version, uint16_t shardIndex) {
    std::vector<uint8_t> out{0, 'A', 'K', 'S', 'S', '1'};
    pushU64(out, version);
    pushU16(out, shardIndex);
    pushU32(out, key.size());
    out.append_range(key);
    return out;
}

static bool isStripeInternalKey(std::span<const uint8_t> key) {
    return key.size() >= 6 && key[0] == 0 && key[1] == 'A' && key[2] == 'K' && key[3] == 'S' && ((key[4] == 'M' && key[5] == '1') || ((key[4] == 'S'
        || key[4] == 'T') && key[5] == '1') || (key[4] == 'H' && key[5] == '2'));
}

static std::optional<std::vector<uint8_t>> publicKeyFromStripeMetaKey(std::span<const uint8_t> key) {
    if (key.size() < 6 || key[0] != 0 || key[1] != 'A' || key[2] != 'K' || key[3] != 'S' || key[4] != 'M' || key[5] != '1') { return std::nullopt; }
    std::vector<uint8_t> out;
    out.assign_range(key.subspan(6));
    return out;
}

static std::vector<uint8_t> encodeStripeMetadata(const StripeMetadata& metadata) {
    std::vector<uint8_t> out{'A', 'K', 'S', 'M', '1'};
    out.push_back((metadata.tombstone ? 1U : 0U) | (metadata.rollback ? 2U : 0U));
    pushU64(out, metadata.version);
    pushU64(out, metadata.ownerNodeId);
    pushU64(out, metadata.authorityNodeId);
    pushU64(out, metadata.fenceToken);
    pushU64(out, metadata.originalSize);
    pushU16(out, metadata.layout.dataShards);
    pushU16(out, metadata.layout.parityShards);
    pushU16(out, metadata.nodeIds.size());
    for (const auto nodeId : metadata.nodeIds) { pushU64(out, nodeId); }
    pushU16(out, metadata.shardPresent.size());
    out.append_range(metadata.shardPresent);
    pushU64(out, metadata.logicalSeq);
    pushU64(out, metadata.originNodeId);
    pushU64(out, metadata.timestampNs);
    return out;
}

static std::optional<StripeMetadata> decodeStripeMetadata(std::span<const uint8_t> bytes) {
    if (bytes.size() < 62 || bytes[0] != 'A' || bytes[1] != 'K' || bytes[2] != 'S' || bytes[3] != 'M' || bytes[4] != '1') { return std::nullopt; }
    size_t cursor = 5;
    StripeMetadata metadata;
    const uint8_t flags = bytes[cursor++];
    if ((flags & ~uint8_t{3}) != 0) { return std::nullopt; }
    metadata.tombstone = (flags & 1U) != 0;
    metadata.rollback = (flags & 2U) != 0;
    if (!pullU64(bytes, cursor, metadata.version) || !pullU64(bytes, cursor, metadata.ownerNodeId) || !
        pullU64(bytes, cursor, metadata.authorityNodeId) || !pullU64(bytes, cursor, metadata.fenceToken) || !
        pullU64(bytes, cursor, metadata.originalSize) || !pullU16(bytes, cursor, metadata.layout.dataShards) || !pullU16(
            bytes,
            cursor,
            metadata.layout.parityShards
        )) { return std::nullopt; }
    uint16_t nodeCount = 0;
    const uint16_t logicalShards = metadata.layout.totalShards();
    if (!pullU16(bytes, cursor, nodeCount) || logicalShards == 0 || nodeCount % logicalShards != 0) { return std::nullopt; }
    const uint16_t copiesPerShard = nodeCount / logicalShards;
    if (copiesPerShard != 1 && copiesPerShard != 2) { return std::nullopt; }
    metadata.copiesPerShard = copiesPerShard;
    metadata.nodeIds.reserve(nodeCount);
    for (uint16_t i = 0; i < nodeCount; ++i) {
        uint64_t nodeId = 0;
        if (!pullU64(bytes, cursor, nodeId)) { return std::nullopt; }
        metadata.nodeIds.push_back(nodeId);
    }
    uint16_t presenceCount = 0;
    if (!pullU16(bytes, cursor, presenceCount) || presenceCount != nodeCount || presenceCount > bytes.size() - cursor) { return std::nullopt; }
    metadata.shardPresent.assign_range(bytes.subspan(cursor, presenceCount));
    if (std::ranges::any_of(metadata.shardPresent, [](uint8_t present) { return present > 1; })) { return std::nullopt; }
    cursor += presenceCount;
    if (!pullU64(bytes, cursor, metadata.logicalSeq) || !pullU64(bytes, cursor, metadata.originNodeId) || !pullU64(
        bytes,
        cursor,
        metadata.timestampNs
    ) || metadata.originNodeId == 0 || metadata.timestampNs == 0) { return std::nullopt; }
    return cursor == bytes.size() ? std::optional<StripeMetadata>{std::move(metadata)} : std::nullopt;
}

static std::vector<uint8_t> encodeStripeShard(uint64_t version, const erasure::ErasureShard& shard, erasure::ErasureLayout layout) {
    std::vector<uint8_t> out{'A', 'K', 'S', 'S', '1'};
    pushU64(out, version);
    pushU16(out, shard.index);
    pushU64(out, shard.originalSize);
    pushU16(out, layout.dataShards);
    pushU16(out, layout.parityShards);
    pushU32(out, shard.crc32c);
    pushU32(out, shard.payload.size());
    out.append_range(shard.payload);
    return out;
}

static std::optional<erasure::ErasureShard> decodeStripeShard(std::span<const uint8_t> bytes, uint64_t version, erasure::ErasureLayout layout) {
    if (bytes.size() < 33 || bytes[0] != 'A' || bytes[1] != 'K' || bytes[2] != 'S' || bytes[3] != 'S' || bytes[4] != '1') { return std::nullopt; }
    size_t cursor = 5;
    uint64_t storedVersion = 0;
    uint16_t index = 0;
    uint64_t originalSize = 0;
    erasure::ErasureLayout storedLayout;
    uint32_t crc = 0;
    uint32_t payloadSize = 0;
    if (!pullU64(bytes, cursor, storedVersion) || !pullU16(bytes, cursor, index) || !pullU64(bytes, cursor, originalSize) || !
        pullU16(bytes, cursor, storedLayout.dataShards) || !pullU16(bytes, cursor, storedLayout.parityShards) || !pullU32(bytes, cursor, crc) || !
        pullU32(bytes, cursor, payloadSize)) { return std::nullopt; }
    if (storedVersion != version || storedLayout.dataShards != layout.dataShards || storedLayout.parityShards != layout.parityShards) {
        return std::nullopt;
    }
    erasure::ErasureShard shard;
    shard.index = index;
    shard.originalSize = originalSize;
    shard.codec = erasure::ErasureCodecKind::RS;
    shard.crc32c = crc;
    if (!pullBytes(bytes, cursor, payloadSize, shard.payload) || cursor != bytes.size() || !shard.verifyCrc()) { return std::nullopt; }
    return shard;
}

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkengine/src/engine/detail/AkkEngineStorage.inc
// Included inside AkkEngine::Impl by AkkEngineImpl.hpp.
[[nodiscard]] std::vector<uint8_t> maybeExternalize(uint64_t seq, std::span<const uint8_t> value, uint8_t& flags) {
    if (!blobManager || value.size() < blobManager->threshold()) { return {value.begin(), value.end()}; }
    if (seq >= LOCAL_BLOB_ID_LIMIT) { throw std::overflow_error("AkkEngine: local Blob sequence is outside encodable range"); }

    blobManager->write(seq, value);
    std::vector<uint8_t> ref(blob::BLOB_REF_SIZE);
    blob::encodeBlobRef(ref.data(), blob::BlobRef{seq, value.size(), blob::crc32c(value)});
    flags |= MemHdr16::FLAG_BLOB;
    if (clusterRuntime) { clusterRuntime->shipBlob(seq, seq, value); }
    return ref;
}

struct StagedSnapshotRecord {
    std::vector<uint8_t> key;
    std::vector<uint8_t> storedValue;
    uint8_t flags = MemHdr16::FLAG_NORMAL;
    uint64_t fp64 = 0;
    uint64_t miniKey = 0;
    bool history = false;
    bool transaction = false;
    cluster::detail::SnapshotRecordKind transactionKind = cluster::detail::SnapshotRecordKind::TRANSACTION;
    uint64_t sequence = 0, source = 0, timestamp = 0;
    uint64_t blobId = 0;
    bool historyPresent = false;
};

static bool streamSnapshotValue(
    const cluster::SnapshotEntryVisitor& visitor,
    std::span<const uint8_t> key,
    std::span<const uint8_t> value,
    uint8_t flags,
    blob::BlobManager* blobs
) {
    if ((flags & MemHdr16::FLAG_BLOB) != 0) {
        if (!blobs || value.size() != blob::BLOB_REF_SIZE) { throw std::runtime_error("AkkEngine: missing snapshot Blob"); }
        const auto ref = blob::decodeBlobRef(value.data());
        if (!visitor.beginEntry(key, ref.totalSize, ref.contentCrc32c)) { return false; }
        if (!blobs->streamRead(
            ref.blobId,
            ref.contentCrc32c,
            ClusterSnapshotExportFile::REPLAY_CHUNK_BYTES,
            [&](uint64_t offset, auto chunk) { return visitor.appendValueChunk(offset, chunk); }
        )) { return false; }
        return visitor.finishEntry();
    }
    Crc32cStream crc;
    crc.update(value);
    if (!visitor.beginEntry(key, value.size(), crc.finish())) { return false; }
    if (value.empty() && !visitor.appendValueChunk(0, {})) { return false; }
    for (size_t offset = 0; offset < value.size();) {
        const auto chunk = value.subspan(offset, std::min(ClusterSnapshotExportFile::REPLAY_CHUNK_BYTES, value.size() - offset));
        if (!visitor.appendValueChunk(offset, chunk)) { return false; }
        offset += chunk.size();
    }
    return visitor.finishEntry();
}

static uint64_t snapshotBlobIdentity(uint8_t flags, std::span<const uint8_t> value) {
    if ((flags & MemHdr16::FLAG_BLOB) == 0) { return 0; }
    if (value.size() != blob::BLOB_REF_SIZE) { throw std::runtime_error("AkkEngine: invalid snapshot Blob reference"); }
    return blob::decodeBlobRef(value.data()).blobId;
}

[[nodiscard]] StagedSnapshotRecord readStagedSnapshotRecord(
    std::istream& in,
    uint64_t seq,
    uint64_t ordinal,
    bool writeBlob,
    uint64_t storedHistoryThrough = 0
) {
    auto [key, valueSize, valueCrc32c, recordCrc] = readSnapshotStagingEntryHeader(in);
    StagedSnapshotRecord record;
    record.key = std::move(key);
    if (clusterConfig.usesDataConsensus()) {
        const auto decoded = cluster::detail::decodeSnapshotKey(record.key);
        if (decoded.kind == cluster::detail::SnapshotRecordKind::STATE || (decoded.kind == cluster::detail::SnapshotRecordKind::HISTORY && (decoded.
            sequence == 0 || decoded.sequence > seq))) { throw std::runtime_error("AkkEngine: invalid history snapshot record"); }
        record.history = decoded.kind == cluster::detail::SnapshotRecordKind::HISTORY;
        record.transaction = decoded.kind >= cluster::detail::SnapshotRecordKind::TRANSACTION;
        record.transactionKind = decoded.kind;
        if (record.transaction && (!distributedTransactionsEnabled() || decoded.flags || decoded.timestamp || decoded.blobId || (
                decoded.kind == cluster::detail::SnapshotRecordKind::TRANSACTION
                    ? !decoded.key.empty() || decoded.sequence || decoded.source
                    : decoded.key.size() != 16 || !decoded.sequence || decoded.sequence > seq || (decoded.kind !=
                        cluster::detail::SnapshotRecordKind::TRANSACTION_PREPARE && !decoded.source)) || valueSize > opts.transactions.
            maxJournalBytes || (decoded.kind == cluster::detail::SnapshotRecordKind::TRANSACTION_PREPARE && valueSize > 1024 * 1024))) {
            throw std::runtime_error("AkkEngine: invalid transaction snapshot record");
        }
        record.sequence = decoded.sequence;
        record.source = decoded.source;
        record.timestamp = decoded.timestamp;
        record.blobId = decoded.blobId;
        record.flags = decoded.flags & ~MemHdr16::FLAG_BLOB;
        record.key = std::vector<uint8_t>{decoded.key.begin(), decoded.key.end()};
        if (record.history) {
            record.historyPresent = record.sequence <= snapshotSeq() || (writeBlob && versionLog && record.sequence <= storedHistoryThrough &&
                versionLog->containsStoredRecord(record.key, record.sequence, record.source, record.timestamp, record.flags));
            if (record.historyPresent) { writeBlob = false; }
        }
    }
    record.fp64 = computeKeyFp64(record.key);
    record.miniKey = buildMiniKey(record.key);

    const bool externalize = !record.transaction && blobManager && valueSize >= blobManager->threshold();
    uint64_t blobId = 0;
    auto blobStarted = false;
    try {
        Crc32cStream valueCrc;
        if (externalize) {
            blobId = record.blobId != 0 ? record.blobId : snapshotBlobId(seq, ordinal);
            record.storedValue.resize(blob::BLOB_REF_SIZE);
            blob::encodeBlobRef(record.storedValue.data(), blob::BlobRef{blobId, valueSize, valueCrc32c});
            record.flags |= MemHdr16::FLAG_BLOB;
            if (writeBlob) {
                blobManager->abortWrite(blobId);
                blobManager->beginWrite(blobId, valueSize, valueCrc32c);
                blobStarted = true;
            }

            std::vector<uint8_t> buffer;
            buffer.resize(std::min<uint64_t>(SNAPSHOT_STAGING_VALUE_CHUNK_BYTES, valueSize));
            uint64_t offset = 0;
            while (offset < valueSize) {
                const size_t chunkSize = std::min<uint64_t>(buffer.size(), valueSize - offset);
                readExact(in, std::span{buffer}.first(chunkSize), "entry value chunk");
                const std::span<const uint8_t> chunk{buffer.data(), chunkSize};
                recordCrc.update(chunk);
                valueCrc.update(chunk);
                if (writeBlob) { blobManager->appendWriteChunk(blobId, offset, chunk); }
                offset += chunkSize;
            }
        }
        else {
            if (valueSize > std::numeric_limits<size_t>::max()) {
                throw std::runtime_error("AkkEngine: replication snapshot staged value is too large");
            }
            record.storedValue.resize(valueSize);
            readExact(in, std::span{record.storedValue}, "entry value");
            recordCrc.update(record.storedValue);
            valueCrc.update(record.storedValue);
        }

        const uint32_t storedRecordCrc = readU32Le(in, "entry crc");
        if (recordCrc.finish() != storedRecordCrc || valueCrc.finish() != valueCrc32c) {
            throw std::runtime_error("AkkEngine: replication snapshot staging entry CRC mismatch");
        }
        if (blobStarted) {
            blobManager->finishWrite(blobId);
            blobStarted = false;
        }
    }
    catch (...) {
        if (blobStarted) { blobManager->abortWrite(blobId); }
        throw;
    }

    if (record.transactionKind == cluster::detail::SnapshotRecordKind::TRANSACTION && record.transaction) {
        (void)decodeDistributedState(record.storedValue);
    }
    if (record.transactionKind == cluster::detail::SnapshotRecordKind::TRANSACTION_DECISION) {
        const auto result = decodeDistributedRecord(record.storedValue);
        if (!result.finished || result.expires != record.source) { throw std::runtime_error("AkkEngine: invalid archived transaction decision"); }
    }
    if (record.transactionKind == cluster::detail::SnapshotRecordKind::TRANSACTION_RECEIPT && !record.storedValue.empty()) {
        throw std::runtime_error("AkkEngine: invalid archived transaction receipt");
    }
    return record;
}

struct PreparedRaftValue {
    std::vector<uint8_t> stored;
    std::optional<cluster::ClusterBlobPayload> blob;
};

[[nodiscard]] PreparedRaftValue prepareRaftValue(uint64_t seq, std::span<const uint8_t> value, uint8_t& flags) {
    if (!blobManager || value.size() < blobManager->threshold()) {
        return PreparedRaftValue{.stored = {value.begin(), value.end()}, .blob = std::nullopt};
    }

    const uint64_t blobId = opts.cluster.runtime.raftBlobPolicy == cluster::RaftBlobPolicy::RAFT_LOG ? raftLogBlobId(nodeId, seq) : seq;
    std::vector<uint8_t> ref(blob::BLOB_REF_SIZE);
    blob::encodeBlobRef(ref.data(), blob::BlobRef{blobId, value.size(), blob::crc32c(value)});
    flags |= MemHdr16::FLAG_BLOB;

    if (opts.cluster.runtime.raftBlobPolicy == cluster::RaftBlobPolicy::RAFT_LOG) {
        return PreparedRaftValue{
            .stored = std::move(ref),
            .blob = cluster::ClusterBlobPayload{.blobId = blobId, .content = {value.begin(), value.end()}},
        };
    }

    blobManager->write(blobId, value);
    return PreparedRaftValue{.stored = std::move(ref), .blob = std::nullopt};
}

[[nodiscard]] std::optional<std::vector<uint8_t>> resolveValue(uint8_t flags, std::span<const uint8_t> value) const {
    if ((flags & MemHdr16::FLAG_BLOB) == 0) { return std::vector<uint8_t>{value.begin(), value.end()}; }
    if (!blobManager || value.size() < blob::BLOB_REF_SIZE) { return std::nullopt; }
    const auto [blobId, totalSize, contentCrc32c] = blob::decodeBlobRef(value.data());
    auto out = blobManager->read(blobId, contentCrc32c);
    if (out.empty() && totalSize != 0) { return std::nullopt; }
    return out;
}

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// jni/detail/StatsObjects.inc
[[nodiscard]] jobject makePlainStats(
    JNIEnv* env,
    const char* className,
    const TStats& /*stats*/,
    std::initializer_list<std::pair<const char*, jlong>> longs,
    std::initializer_list<std::pair<const char*, bool>> bools = {}
) {
    jclass cls = env->FindClass(className);
    if (cls == nullptr) { return nullptr; }
    jmethodID ctor = env->GetMethodID(cls, "<init>", "()V");
    if (ctor == nullptr) { return nullptr; }
    jobject out = env->NewObject(cls, ctor);
    if (out == nullptr || env->ExceptionCheck()) { return nullptr; }
    for (const auto& [name, value] : bools) { setBooleanField(env, out, cls, name, value); }
    for (const auto& [name, value] : longs) { setLongField(env, out, cls, name, value); }
    return out;
}

[[nodiscard]] jobject makeEngineStats(JNIEnv* env, const akkaradb::engine::EngineStats& stats) {
    jclass cls = env->FindClass("dev/swiftstorm/akkaradb/engine/EngineStats");
    if (cls == nullptr) { return nullptr; }
    jmethodID ctor = env->GetMethodID(cls, "<init>", "()V");
    if (ctor == nullptr) { return nullptr; }
    jobject out = env->NewObject(cls, ctor);
    if (out == nullptr || env->ExceptionCheck()) { return nullptr; }

    jobject api = makeApiStats(env, stats.api);
    jobject memtable = makePlainStats(
        env,
        "dev/swiftstorm/akkaradb/engine/MemTableStats",
        stats.memtable,
        {
            {"shardCount", static_cast<jlong>(stats.memtable.shardCount)},
            {"thresholdBytesPerShard", static_cast<jlong>(stats.memtable.thresholdBytesPerShard)},
            {"approxBytes", static_cast<jlong>(stats.memtable.approxBytes)},
            {"putsApplied", static_cast<jlong>(stats.memtable.putsApplied)},
            {"removesApplied", static_cast<jlong>(stats.memtable.removesApplied)},
            {"flushesCompleted", static_cast<jlong>(stats.memtable.flushesCompleted)},
            {"bytesFlushed", static_cast<jlong>(stats.memtable.bytesFlushed)}
        }
    );
    jobject wal = makePlainStats(
        env,
        "dev/swiftstorm/akkaradb/engine/WalStats",
        stats.wal,
        {
            {"shardCount", static_cast<jlong>(stats.wal.shardCount)},
            {"entriesWritten", static_cast<jlong>(stats.wal.entriesWritten)},
            {"bytesWritten", static_cast<jlong>(stats.wal.bytesWritten)},
            {"batchesFlushed", static_cast<jlong>(stats.wal.batchesFlushed)},
            {"syncsExecuted", static_cast<jlong>(stats.wal.syncsExecuted)},
            {"segmentRotations", static_cast<jlong>(stats.wal.segmentRotations)}
        },
        {{"enabled", stats.wal.enabled}}
    );
    jobject blob = makePlainStats(
        env,
        "dev/swiftstorm/akkaradb/engine/BlobStats",
        stats.blob,
        {
            {"thresholdBytes", static_cast<jlong>(stats.blob.thresholdBytes)},
            {"blobsWritten", static_cast<jlong>(stats.blob.blobsWritten)},
            {"bytesUncompressed", static_cast<jlong>(stats.blob.bytesUncompressed)},
            {"bytesOnDisk", static_cast<jlong>(stats.blob.bytesOnDisk)},
            {"blobsDeleted", static_cast<jlong>(stats.blob.blobsDeleted)},
            {"gcCycles", static_cast<jlong>(stats.blob.gcCycles)}
        },
        {{"enabled", stats.blob.enabled}}
    );
    jobject vlog = makePlainStats(
        env,
        "dev/swiftstorm/akkaradb/engine/VLogStats",
        stats.vlog,
        {
            {"syncMode", static_cast<jlong>(stats.vlog.syncMode)},
            {"groupN", static_cast<jlong>(stats.vlog.groupN)},
            {"groupMicros", static_cast<jlong>(stats.vlog.groupMicros)},
            {"groupBytes", static_cast<jlong>(stats.vlog.groupBytes)},
            {"asyncMaxPendingBytes", static_cast<jlong>(stats.vlog.asyncMaxPendingBytes)},
            {"indexedKeys", static_cast<jlong>(stats.vlog.indexedKeys)},
            {"indexedEntries", static_cast<jlong>(stats.vlog.indexedEntries)},
            {"rollbackEntries", static_cast<jlong>(stats.vlog.rollbackEntries)},
            {"pendingWrites", static_cast<jlong>(stats.vlog.pendingWrites)},
            {"pendingBytes", static_cast<jlong>(stats.vlog.pendingBytes)},
            {"durableBytes", static_cast<jlong>(stats.vlog.durableBytes)},
            {"segmentCount", static_cast<jlong>(stats.vlog.segmentCount)},
            {"activeSegmentBytes", static_cast<jlong>(stats.vlog.activeSegmentBytes)},
            {"recoveryDurationMicros", static_cast<jlong>(stats.vlog.recoveryDurationMicros)},
            {"recoveredSegmentCount", static_cast<jlong>(stats.vlog.recoveredSegmentCount)},
            {"recoveredEntryCount", static_cast<jlong>(stats.vlog.recoveredEntryCount)},
            {"sidecarFallbackCount", static_cast<jlong>(stats.vlog.sidecarFallbackCount)},
            {"sidecarRebuildFailures", static_cast<jlong>(stats.vlog.sidecarRebuildFailures)},
            {"retentionPrunedSegments", static_cast<jlong>(stats.vlog.retentionPrunedSegments)},
            {"retentionBaseEntriesWritten", static_cast<jlong>(stats.vlog.retentionBaseEntriesWritten)},
            {"parallelQueueRejects", static_cast<jlong>(stats.vlog.parallelQueueRejects)},
            {"parallelLaneCount", static_cast<jlong>(stats.vlog.parallelLaneCount)},
            {"parallelPendingWrites", static_cast<jlong>(stats.vlog.parallelPendingWrites)},
            {"parallelPendingBytes", static_cast<jlong>(stats.vlog.parallelPendingBytes)}
        },
        {{"enabled", stats.vlog.enabled}, {"flushThreadRunning", stats.vlog.flushThreadRunning}}
    );
    jobject manifest = makePlainStats(
        env,
        "dev/swiftstorm/akkaradb/engine/ManifestStats",
        stats.manifest,
        {
            {"lastCheckpointSeq", static_cast<jlong>(stats.manifest.lastCheckpointSeq)},
            {"lastCheckpointStripe", static_cast<jlong>(stats.manifest.lastCheckpointStripe)},
            {"liveSstCount", static_cast<jlong>(stats.manifest.liveSstCount)},
            {"deletedSstCount", static_cast<jlong>(stats.manifest.deletedSstCount)},
            {"sstSealCount", static_cast<jlong>(stats.manifest.sstSealCount)},
            {"sstReferencedBlobCount", static_cast<jlong>(stats.manifest.sstReferencedBlobCount)},
            {"liveBlobCount", static_cast<jlong>(stats.manifest.liveBlobCount)},
            {"deletedBlobCount", static_cast<jlong>(stats.manifest.deletedBlobCount)},
            {"blobPutCount", static_cast<jlong>(stats.manifest.blobPutCount)},
            {"blobDeleteCount", static_cast<jlong>(stats.manifest.blobDeleteCount)},
            {"lastPrimaryLeaseNodeId", static_cast<jlong>(stats.manifest.lastPrimaryLeaseNodeId)},
            {"lastPrimaryLeaseUntilUs", static_cast<jlong>(stats.manifest.lastPrimaryLeaseUntilUs)}
        },
        {
            {"enabled", stats.manifest.enabled},
            {"hasCheckpoint", stats.manifest.hasCheckpoint},
            {"sstBlobRefsComplete", stats.manifest.sstBlobRefsComplete}
        }
    );
    jobject cluster = makePlainStats(
        env,
        "dev/swiftstorm/akkaradb/engine/ClusterStats",
        stats.cluster,
        {
            {"role", static_cast<jlong>(stats.cluster.role)},
            {"configuredNodeCount", static_cast<jlong>(stats.cluster.configuredNodeCount)},
            {"activeNodeCount", static_cast<jlong>(stats.cluster.activeNodeCount)}
        },
        {{"enabled", stats.cluster.enabled}}
    );

    jclass sstCls = env->FindClass("dev/swiftstorm/akkaradb/engine/SstStats");
    if (sstCls == nullptr) { return nullptr; }
    jmethodID sstCtor = env->GetMethodID(sstCls, "<init>", "()V");
    if (sstCtor == nullptr) { return nullptr; }
    jobject sst = env->NewObject(sstCls, sstCtor);
    if (sst == nullptr || env->ExceptionCheck()) { return nullptr; }
    jclass levelCls = env->FindClass("dev/swiftstorm/akkaradb/engine/LevelStats");
    if (levelCls == nullptr) { return nullptr; }
    jobjectArray levels = env->NewObjectArray(static_cast<jsize>(stats.sst.levels.size()), levelCls, nullptr);
    if (levels == nullptr || env->ExceptionCheck()) { return nullptr; }
    for (jsize i = 0; i < static_cast<jsize>(stats.sst.levels.size()); ++i) {
        jobject level = makeLevelStats(env, stats.sst.levels[static_cast<size_t>(i)]);
        if (level == nullptr || env->ExceptionCheck()) { return nullptr; }
        env->SetObjectArrayElement(levels, i, level);
        if (env->ExceptionCheck()) { return nullptr; }
        env->DeleteLocalRef(level);
    }
    setBooleanField(env, sst, sstCls, "enabled", stats.sst.enabled);
    setObjectField(env, sst, sstCls, "levels", "[Ldev/swiftstorm/akkaradb/engine/LevelStats;", levels);
    setLongField(env, sst, sstCls, "fileCount", static_cast<jlong>(stats.sst.fileCount));
    setLongField(env, sst, sstCls, "bytes", static_cast<jlong>(stats.sst.bytes));
    setLongField(env, sst, sstCls, "l0FileCount", static_cast<jlong>(stats.sst.l0FileCount));
    setBooleanField(env, sst, sstCls, "compactionPending", stats.sst.compactionPending);
    setLongField(env, sst, sstCls, "compactionsCompleted", static_cast<jlong>(stats.sst.compactionsCompleted));
    setLongField(env, sst, sstCls, "filesCompacted", static_cast<jlong>(stats.sst.filesCompacted));
    setLongField(env, sst, sstCls, "bytesCompactedIn", static_cast<jlong>(stats.sst.bytesCompactedIn));
    setLongField(env, sst, sstCls, "bytesCompactedOut", static_cast<jlong>(stats.sst.bytesCompactedOut));
    setLongField(env, sst, sstCls, "l0Stalls", static_cast<jlong>(stats.sst.l0Stalls));

    setLongField(env, out, cls, "currentSeq", static_cast<jlong>(stats.currentSeq));
    setLongField(env, out, cls, "nodeId", static_cast<jlong>(stats.nodeId));
    setLongField(env, out, cls, "putsTotal", static_cast<jlong>(stats.putsTotal));
    setLongField(env, out, cls, "removesTotal", static_cast<jlong>(stats.removesTotal));
    setLongField(env, out, cls, "getsTotal", static_cast<jlong>(stats.getsTotal));
    setLongField(env, out, cls, "getsMemtableHit", static_cast<jlong>(stats.getsMemtableHit));
    setLongField(env, out, cls, "getsSstHit", static_cast<jlong>(stats.getsSstHit));
    setLongField(env, out, cls, "getsMiss", static_cast<jlong>(stats.getsMiss));
    setLongField(env, out, cls, "existsTotal", static_cast<jlong>(stats.existsTotal));
    setLongField(env, out, cls, "scansTotal", static_cast<jlong>(stats.scansTotal));
    setLongField(env, out, cls, "blobPutsTotal", static_cast<jlong>(stats.blobPutsTotal));
    setObjectField(env, out, cls, "api", "Ldev/swiftstorm/akkaradb/engine/ApiStats;", api);
    setObjectField(env, out, cls, "memtable", "Ldev/swiftstorm/akkaradb/engine/MemTableStats;", memtable);
    setObjectField(env, out, cls, "wal", "Ldev/swiftstorm/akkaradb/engine/WalStats;", wal);
    setObjectField(env, out, cls, "blob", "Ldev/swiftstorm/akkaradb/engine/BlobStats;", blob);
    setObjectField(env, out, cls, "sst", "Ldev/swiftstorm/akkaradb/engine/SstStats;", sst);
    setObjectField(env, out, cls, "manifest", "Ldev/swiftstorm/akkaradb/engine/ManifestStats;", manifest);
    setObjectField(env, out, cls, "cluster", "Ldev/swiftstorm/akkaradb/engine/ClusterStats;", cluster);
    setObjectField(env, out, cls, "vlog", "Ldev/swiftstorm/akkaradb/engine/VLogStats;", vlog);
    return out;
}

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// jni/detail/NativeExportsScanHistory.inc
    JNIEXPORT jlong JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeOpenScan(
    JNIEnv* env,
    jobject,
    jlong handle,
    jobject startKey,
    jobject endKey
) {
    return guard(
        env,
        [&]() -> jlong {
            auto* db = dbFrom(handle);
            if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
            const auto start = readDirectBuffer(env, startKey);
            const auto end = readDirectBuffer(env, endKey);

            auto cursor = std::make_unique<ScanCursor>();
            cursor->rows = db->engine().scan(cursor->arena, start, end);
            cursor->it = cursor->rows.begin();
            return toHandle(cursor.release());
        }
    );
}

    JNIEXPORT jlong JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeOpenQueryScan(
    JNIEnv* env,
    jobject,
    jlong handle,
    jobject startKey,
    jobject endKey,
    jobject queryBytes,
    jobject schemaBytes
) {
    return guard(
        env,
        [&]() -> jlong {
            auto* db = dbFrom(handle);
            if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
            const auto start = readDirectBuffer(env, startKey);
            const auto end = readDirectBuffer(env, endKey);
            const auto query = readDirectBuffer(env, queryBytes);
            const auto schema = readDirectBuffer(env, schemaBytes);

            auto cursor = std::make_unique<ScanCursor>();
            cursor->query = std::make_unique<RuntimeBytecodeQuery>(parseQuery(query, schema));
            cursor->rows = db->engine().scan(cursor->arena, start, end);
            cursor->it = cursor->rows.begin();
            return toHandle(cursor.release());
        }
    );
}

    JNIEXPORT jobject JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeGetAt(
    JNIEnv* env,
    jobject,
    jlong handle,
    jobject key,
    jlong targetSeq
) {
    return guard(
        env,
        [&]() -> jobject {
            auto* db = dbFrom(handle);
            if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
            const auto k = readDirectBuffer(env, key);
            auto value = db->engine().getAt(k, static_cast<uint64_t>(targetSeq));
            if (!value) { return nullptr; }
            return makeDirectBuffer(env, *value);
        }
    );
}

    JNIEXPORT jobjectArray JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeHistory(
    JNIEnv* env,
    jobject,
    jlong handle,
    jobject key
) {
    return guard(
        env,
        [&]() -> jobjectArray {
            auto* db = dbFrom(handle);
            if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
            const auto k = readDirectBuffer(env, key);
            const auto history = db->engine().history(k);
            const auto& ids = bindings(env);
            if (history.size() > static_cast<size_t>(std::numeric_limits<jsize>::max())) {
                throw std::runtime_error("AkkaraDB JNI history result is too large");
            }
            jobjectArray out = env->NewObjectArray(static_cast<jsize>(history.size()), ids.versionEntryClass, nullptr);
            if (out == nullptr || env->ExceptionCheck()) { return nullptr; }
            for (jsize i = 0; i < static_cast<jsize>(history.size()); ++i) {
                jobject entry = makeVersionEntry(env, history[static_cast<size_t>(i)]);
                if (entry == nullptr || env->ExceptionCheck()) { return nullptr; }
                env->SetObjectArrayElement(out, i, entry);
                if (env->ExceptionCheck()) { return nullptr; }
                env->DeleteLocalRef(entry);
            }
            return out;
        }
    );
}

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeRollbackTo(
    JNIEnv* env,
    jobject,
    jlong handle,
    jlong targetSeq
) {
    guard(
        env,
        [&]() {
            auto* db = dbFrom(handle);
            if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
            db->engine().rollbackTo(static_cast<uint64_t>(targetSeq));
            return 0;
        }
    );
}

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeRollbackKey(
    JNIEnv* env,
    jobject,
    jlong handle,
    jobject key,
    jlong targetSeq
) {
    guard(
        env,
        [&]() {
            auto* db = dbFrom(handle);
            if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
            db->engine().rollbackKey(readDirectBuffer(env, key), static_cast<uint64_t>(targetSeq));
            return 0;
        }
    );
}

    JNIEXPORT jobject JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeStats(JNIEnv* env, jobject, jlong handle) {
    return guard(
        env,
        [&]() -> jobject {
            auto* db = dbFrom(handle);
            if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
            return makeEngineStats(env, db->engine().stats());
        }
    );
}

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeForceSync(JNIEnv* env, jobject, jlong handle) {
    guard(
        env,
        [&]() {
            auto* db = dbFrom(handle);
            if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
            db->engine().forceSync();
            return 0;
        }
    );
}

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeForceFlush(JNIEnv* env, jobject, jlong handle) {
    guard(
        env,
        [&]() {
            auto* db = dbFrom(handle);
            if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
            db->engine().forceFlush();
            return 0;
        }
    );
}

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeRunBlobGc(JNIEnv* env, jobject, jlong handle) {
    guard(
        env,
        [&]() {
            auto* db = dbFrom(handle);
            if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
            db->engine().runBlobGc();
            return 0;
        }
    );
}

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeClose(JNIEnv* env, jobject, jlong handle) {
    guard(
        env,
        [&]() {
            std::unique_ptr<AkkaraDB> db{dbFrom(handle)};
            if (db) { db->close(); }
            return 0;
        }
    );
}

    JNIEXPORT jobject JNICALL Java_dev_swiftstorm_akkaradb_engine_NativeScanCursor_nativeNext(JNIEnv* env, jobject, jlong handle) {
    return guard(
        env,
        [&]() -> jobject {
            auto* cursor = cursorFrom(handle);
            if (cursor == nullptr) { throw std::runtime_error("NativeScanCursor handle is null"); }
            while (cursor->it != cursor->rows.end()) {
                const auto& row = *cursor->it;
                if (cursor->indexLookup != nullptr) {
                    const auto& state = *cursor->indexLookup;
                    const auto key = row.key;
                    ++cursor->it;
                    if (key.size() <= state.searchPrefixSize) { continue; }
                    const std::span<const uint8_t> pkBytes{key.data() + state.searchPrefixSize, key.size() - state.searchPrefixSize};
                    const auto pkKey = concatBytes(state.tablePrefix, pkBytes);
                    auto value = state.db->engine().get(pkKey);
                    if (!value) { continue; }
                    if (cursor->query != nullptr && !matchesQuery(*cursor->query, *value)) { continue; }
                    return makeRow(env, pkKey, *value);
                }
                const bool matched = cursor->query == nullptr || matchesQuery(*cursor->query, row.value);
                if (matched) {
                    jobject out = makeRow(env, row.key, row.value);
                    ++cursor->it;
                    return out;
                }
                ++cursor->it;
            }
            return nullptr;
        }
    );
}

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_NativeScanCursor_nativeClose(JNIEnv* env, jobject, jlong handle) {
    guard(
        env,
        [&]() {
            std::unique_ptr<ScanCursor> cursor{cursorFrom(handle)};
            return 0;
        }
    );
}

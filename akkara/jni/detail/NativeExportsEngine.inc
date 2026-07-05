
    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativePut(
    JNIEnv* env,
    jobject,
    jlong handle,
    jobject key,
    jobject value
) {
    guard(
        env,
        [&]() {
            auto* db = dbFrom(handle);
            if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
            const auto k = readDirectBuffer(env, key);
            const auto v = readDirectBuffer(env, value);
            db->engine().put(k, v);
            return 0;
        }
    );
}

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativePutHinted(
    JNIEnv* env,
    jobject,
    jlong handle,
    jobject key,
    jobject value,
    jlong fp64,
    jlong miniKey
) {
    guard(
        env,
        [&]() {
            auto* db = dbFrom(handle);
            if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
            db->engine().putHinted(
                readDirectBuffer(env, key),
                readDirectBuffer(env, value),
                static_cast<uint64_t>(fp64),
                static_cast<uint64_t>(miniKey)
            );
            return 0;
        }
    );
}

    JNIEXPORT jobject JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeGet(JNIEnv* env, jobject, jlong handle, jobject key) {
    return guard(
        env,
        [&]() -> jobject {
            auto* db = dbFrom(handle);
            if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
            const auto k = readDirectBuffer(env, key);
            auto value = db->engine().get(k);
            if (!value) { return nullptr; }
            return makeDirectBuffer(env, *value);
        }
    );
}

    JNIEXPORT jlong JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeOpenValueView(
    JNIEnv* env,
    jobject,
    jlong handle,
    jobject key
) {
    return guard(
        env,
        [&]() -> jlong {
            auto* db = dbFrom(handle);
            if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
            const auto k = readDirectBuffer(env, key);
            auto view = std::make_unique<NativeValueView>();
            if (!db->engine().getIntoArena(k, view->arena, view->value)) { return 0; }
            return toHandle(view.release());
        }
    );
}

    JNIEXPORT jobject JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeValueViewBuffer(
    JNIEnv* env,
    jobject,
    jlong viewHandle
) {
    return guard(
        env,
        [&]() -> jobject {
            auto* view = reinterpret_cast<NativeValueView*>(static_cast<std::uintptr_t>(viewHandle));
            if (view == nullptr) { throw std::runtime_error("Native value view handle is null"); }
            return makeDirectView(env, view->value);
        }
    );
}

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeCloseValueView(JNIEnv* env, jobject, jlong viewHandle) {
    guard(
        env,
        [&]() {
            delete reinterpret_cast<NativeValueView*>(static_cast<std::uintptr_t>(viewHandle));
            return 0;
        }
    );
}

    JNIEXPORT jobjectArray JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeGetBatch(
    JNIEnv* env,
    jobject,
    jlong handle,
    jobjectArray keys
) {
    return guard(
        env,
        [&]() -> jobjectArray {
            auto* db = dbFrom(handle);
            if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
            std::vector<std::span<const uint8_t>> keySpans;
            readDirectBufferArray(env, keys, keySpans);
            const auto results = db->engine().getBatch(keySpans);
            const auto& ids = bindings(env);
            jobjectArray out = env->NewObjectArray(static_cast<jsize>(results.size()), ids.batchGetResultClass, nullptr);
            if (out == nullptr || env->ExceptionCheck()) { return nullptr; }
            for (jsize i = 0; i < static_cast<jsize>(results.size()); ++i) {
                jobject result = makeBatchGetResult(env, results[static_cast<size_t>(i)]);
                if (result == nullptr || env->ExceptionCheck()) { return nullptr; }
                env->SetObjectArrayElement(out, i, result);
                if (env->ExceptionCheck()) { return nullptr; }
                env->DeleteLocalRef(result);
            }
            return out;
        }
    );
}

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativePutBatch(
    JNIEnv* env,
    jobject,
    jlong handle,
    jobjectArray keys,
    jobjectArray values
) {
    guard(
        env,
        [&]() {
            auto* db = dbFrom(handle);
            if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
            const jsize keyCount = env->GetArrayLength(keys);
            const jsize valueCount = env->GetArrayLength(values);
            if (keyCount != valueCount) { throw std::runtime_error("putBatch keys and values length mismatch"); }
            std::vector<std::span<const uint8_t>> keySpans;
            std::vector<std::span<const uint8_t>> valueSpans;
            readDirectBufferArray(env, keys, keySpans);
            readDirectBufferArray(env, values, valueSpans);
            std::vector<AkkEngine::BatchPutEntry> entries;
            entries.reserve(static_cast<size_t>(keyCount));
            for (jsize i = 0; i < keyCount; ++i) {
                entries.push_back({keySpans[static_cast<size_t>(i)], valueSpans[static_cast<size_t>(i)]});
            }
            db->engine().putBatch(entries);
            return 0;
        }
    );
}

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeRemove(JNIEnv* env, jobject, jlong handle, jobject key) {
    guard(
        env,
        [&]() {
            auto* db = dbFrom(handle);
            if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
            const auto k = readDirectBuffer(env, key);
            db->engine().remove(k);
            return 0;
        }
    );
}

    JNIEXPORT void JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeRemoveHinted(
    JNIEnv* env,
    jobject,
    jlong handle,
    jobject key,
    jlong fp64,
    jlong miniKey
) {
    guard(
        env,
        [&]() {
            auto* db = dbFrom(handle);
            if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
            db->engine().removeHinted(readDirectBuffer(env, key), static_cast<uint64_t>(fp64), static_cast<uint64_t>(miniKey));
            return 0;
        }
    );
}

    JNIEXPORT jboolean JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeExists(JNIEnv* env, jobject, jlong handle, jobject key) {
    return guard(
        env,
        [&]() -> jboolean {
            auto* db = dbFrom(handle);
            if (db == nullptr) { throw std::runtime_error("AkkEngine handle is null"); }
            const auto k = readDirectBuffer(env, key);
            return db->engine().exists(k) ? JNI_TRUE : JNI_FALSE;
        }
    );
}

    JNIEXPORT jlong JNICALL Java_dev_swiftstorm_akkaradb_engine_AkkEngine_nativeCount(
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
            return static_cast<jlong>(db->engine().count(start, end));
        }
    );
}
